#!/usr/bin/env python

# KV Store Pull
# Enables the download of remote collections to a local SH instance on a per-app basis
# Pulls collections from a remote search head or SHC node to the local SH KV store

# Author: J.R. Murray <jr.murray@deductiv.net>
# Version: 2.0.0

from future import standard_library
standard_library.install_aliases()
from builtins import str
import sys
from splunk.clilib import cli_common as cli
from splunklib.searchcommands import \
	dispatch, GeneratingCommand, Configuration, Option, validators
from splunk.clilib import cli_common as cli
import splunklib.client as client
import splunk.rest as rest
import splunk.entity as entity
import os, stat
import json
import http.client, urllib.request, urllib.parse, urllib.error
import urllib.request, urllib.error, urllib.parse
import time
from datetime import datetime
import gzip
import glob
import shutil
import logging
import re
from xml.dom import minidom
import roles
import kv_collection_copy as kv
from deductiv_helpers import *

@Configuration()
class KVStorePullCommand(GeneratingCommand):
	""" %(synopsis)

	##Syntax

	| kvstorepush app="app_name" collection="collection_name" global_scope="false" target="remotehost"

	##Description

	Upload each collection in the KV Store to a remote Splunk Search Head/SHC instance

	"""

	app = Option(
		doc='''
			Syntax: app=<appname>
			Description: Specify the app to download collections from''',
			require=False)

	global_scope = Option(
		doc='''
			Syntax: global_scope=[true|false]
			Description: Specify the whether or not to include all globally available collections''',
			require=False, validate=validators.Boolean())

	collection = Option(
		doc='''
			Syntax: collection=<collection_name>
			Description: Specify the collection to backup within the specified app''',
			require=False)

	append = Option(
		doc='''
			Syntax: append=[true|false]
			Description: Specify whether or not to delete existing entries on the target.''',
			require=False, validate=validators.Boolean())

	target = Option(
		doc='''
			Syntax: target=<remotetarget_hostname>
			Description: Specify the hostname to download from. Credentials must be given via setup.''',
			require=True)

	targetport = Option(
		doc='''
			Syntax: port=<Port>
			Description: Specify the Splunk serviceport''',
			require=False, validate=validators.Integer(minimum=1,maximum=65535))

	def generate(self):
		logger = logging.getLogger('kvst')

		# Facility Info in the loglines
		facility = os.path.basename(__file__)
		facility = os.path.splitext(facility)[0]
		#facility = facility.replace('_','')
		facility = "[%s]" % (facility)

		logger.debug('%s KVStorePullCommand: %s', facility, self)
		errors = []

		# Check permissions
		required_role = "kv_admin"
		active_user = self._metadata.searchinfo.username
		if active_user in roles.get_role_users(self._metadata.searchinfo.session_key, required_role) or active_user == "admin":
			logger.debug("%s User %s is authorized.", facility, active_user)
		else:
			logger.error("%s User %s is unauthorized. Has the kv_admin role been granted?", facility, active_user)
			yield({'Error': 'User %s is unauthorized. Has the kv_admin role been granted?' % active_user })
			sys.exit(3)

		logger.info('%s kvstorepull started', facility)

		try:
			cfg = cli.getConfStanza('kvstore_tools','backups')
			limits_cfg = cli.getConfStanza('limits','kvstore')
		except BaseException as e:
			logger.error("%s ERROR getting configuration: " + str(e), facility)

		batch_size = int(cfg.get('backup_batch_size'))
		logger.debug("%s Batch size: %d rows", facility, batch_size)

		local_session_key = self._metadata.searchinfo.session_key
		splunkd_uri = self._metadata.searchinfo.splunkd_uri

		# Sanitize input
		if self.app:
			logger.debug('%s App Context: %s', facility, self.app)
		else:
			self.app = None

		if self.collection:
			logger.debug('%s Collection: %s', facility, self.collection)
		else:
			self.collection=None

		if self.global_scope:
			logger.debug('%s Global Scope: %s', facility, self.global_scope)
		else:
			self.global_scope = False

		if self.append:
			logger.debug('%s Appending to existing collection', facility)
		else:
			self.append = False
			logger.debug('%s Append to existing collection: ' + str(self.append), facility)

		if self.targetport:
			logger.debug('%s Port for remote connect: %s', facility, self.targetport)
		else:
			self.targetport = 8089

		# Get credentials
		try:
			# Use the credential where the realm matches the target hostname
			# Otherwise, use the last entry in the list
			credentials_list = get_credentials('kvstore_tools', local_session_key)
			for cred in credentials_list:
				if 'realm' in list(cred.keys()):
					if cred['realm'] == self.target:
						credential = cred
			if credential is None:
				credential = credentials_list[-1]
			
			remote_user = credential['username']
			remote_password = credential['password']

		except BaseException as e:
			logger.critical('%s ERROR Failed to get credentials for remote Splunk instance: %s', facility, str(e))
			yield({'Error': 'Failed to get credentials for remote Splunk instance: ' + str(e)})
		
		url_tmpl_app = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/config?output_mode=json&count=0'

		# Login to the remote host and get the session key
		try:
			remote_host = self.target
			remote_port = self.targetport
			remote_uri = 'https://' + self.target + ':' + str(self.targetport)
			
			remote_service = client.connect(
				host=remote_host,
				port=remote_port,
				username=remote_user,
				password=remote_password)
			remote_service.login()

			remote_session_key = remote_service.token
			logger.debug(remote_session_key)
			logger.debug('%s Remote Session_key: %s' % (facility,  remote_session_key))
			
		except (urllib.error.HTTPError, BaseException) as e:
			logger.exception('%s ERROR Failed to login on remote Splunk instance: %s', facility, str(e))
			yield({'Error': 'Failed to login on remote Splunk instance: ' + str(e)})
			sys.exit(4424)

		try:
			remote_apps = kv.get_server_apps(remote_uri, remote_session_key, self.app)
		except BaseException as e:
			logger.critical('%s Failed to create app list: %s', facility, repr(e))
			yield( {'Error': 'Failed to create app list: %s' % repr(e)} )
			sys.exit(719)
		
		# collections = [[app, collection_name], ...]
		collections = kv.enumerate_collections(remote_uri, remote_session_key, self.collection, self.app, remote_apps, self.global_scope)
		logger.debug('%s Collections to pull: %s', facility, str(collections))


		for collection in collections:
			# Extract the app and collection name from the array
			collection_app = collection[0]
			collection_name = collection[1]
			try:
				results = kv.copy_collection(logger, remote_uri, splunkd_uri, collection_app, collection_name, self.append)
			except BaseException as e:
				logger.critical('%s Failed to copy collections from %s to local KV store: %s', facility, self.target, e.read() )
				yield({'Error': 'Failed to download remote collection list: %s' % e.read() } )
				sys.exit(11)
			"""
			yield {'_time': time.time(), 'app': collection_app, 'collection': collection_name, 'result': result, 'records': total_record_count, 'message': message }
			content_len = len(batched_response)
			logger.debug('%s Length batched_response: ' + str(len(batched_response)), facility)
			#logger.debug('BATCHED RESPONSE: ' + str(batched_response))

			# Set URL templates
			url_tmpl_add_collection = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/config?output_mode=json'
			url_tmpl_batch = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/data/%(collection)s/batch_save?output_mode=json'
			url_tmpl = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/data/%(collection)s/?output_mode=json'

			# Build the URL for adding the collection
			create_collection_url = url_tmpl_add_collection % dict(
				server_uri=remote_uri,
				owner='nobody',
				app=collection_app)

			# Build the URL for deleting the collection
			delete_url = url_tmpl % dict(
				server_uri=remote_uri,
				owner='nobody',
				app=collection_app,
				collection=collection_name)

			# Build the URL for updating the collection
			record_url = url_tmpl_batch % dict(
				server_uri=remote_uri,
				owner='nobody',
				app=collection_app,
				collection=collection_name)

			logger.debug('%s delete_url: ' + delete_url + ' record_url: ' + record_url, facility)

			# Use remote session_key for target Host
			headers = {
				'Authorization': remote_session_key,
				'Content-Type': 'application/json'}

			# Enumerate all of the collections in the app (if an app is selected)
			collections_url = url_tmpl_app % dict(
				server_uri=remote_uri,
				owner='nobody',
				app=app)

			# Get list of collections on the remote-host
			try:
				response = request('GET', collections_url, '', headers)
			except BaseException as e:
				logger.critical('%s ERROR Failed to download remote collection list: %s', facility, e.read() )
				yield({'Error': 'Failed to download remote collection list: %s' % e.read() } )
				sys.exit(11)
			
			# Look for collection in remote-collection-list, create it if necessary
			if not any(d['name'] == collection_name for d in response["entry"]):
				try:
					response = self.request('POST',create_collection_url,'name=' + str(collection[1]),headers)
					logger.debug('%s Created collection: %s', facility, str(collection[1]))
				except urllib.error.URLError as e:
					logger.critical('%s ERROR URLError = %s', facility, repr(e))
					yield({'Error': 'URL Error: %s' % repr(e) })
					sys.exit(15)
				except http.client.HTTPException as e:
					logger.critical('%s HTTPException: %s', facility, repr(e))
					yield({'Error': 'HTTP Exception: %s' % repr(e) })
					sys.exit(16)
				except BaseException as e:
					logger.critical('%s ERROR Failed to create collection: %s', facility, str(collection[1]))
					yield({'Error': 'Failed to create collection: %s' % repr(e) })
					sys.exit(14)
			else:
				if not self.append:
					# Delete the collection contents
					try:
						response = self.request('DELETE', delete_url, '', headers)
						logger.debug('%s Server response for collection deletion: %s', facility, json.dumps(response))
					except urllib.error.HTTPError as e:
						logger.critical('%s ERROR Failed to delete collection: %s', facility, e.read())
						yield({'Error': 'Failed to delete collection: %s' % e.read() })
						sys.exit(17)
					except urllib.error.URLError as e:
						logger.critical('%s ERROR URLError = %s', facility, repr(e))
						yield({'Error': 'URL Error: %s' % repr(e) })
						sys.exit(18)
					except http.client.HTTPException as e:
						logger.critical('%s HTTPException: %s' + repr(e), facility)
						yield({'Error': 'HTTP Exception: %s' % repr(e) })
						sys.exit(19)

			# set everything up for the Upload
			i = 0
			batch_number = 1
			limit = int(limits_cfg.get('max_documents_per_batch_save'))
			posted = 0

			while i < content_len:
				# Get the lesser number between (limit-1) and (content_len)
				last = (batch_number*limit)
				last = min(last, content_len)
				batch = batched_response[i:last]
				i += limit

				logger.debug('%s Batch number: %d (%d bytes)', facility, batch_number, sys.getsizeof(batch) )

				# Upload the restored records to the server
				try:
					#logger.debug('posting batch: ' + json.dumps(batch))
					response = self.request('POST', record_url, json.dumps(batch), headers)
					logger.debug('%s Server response: %d records uploaded.', facility, len(response))
					batch_number += 1
					posted += len(batch)
					message = 'posted collection to ' + self.target
					result = 'success'
				except urllib.error.HTTPError as e:
					logger.error('%s ERROR Failed to update records: %s', facility, e.read())
					message = str(e)
					result = 'error'
					# Force out of the while loop
					i = content_len
				except urllib.error.URLError as e:
					logger.critical('%s ERROR URLError = %s', facility, repr(e))
					message = str(e)
					result = 'critical'
					yield {'_time': time.time(), 'app': app, 'collection': collection_name, 'records': posted, 'result': result, 'message': message }
					# exit because we could have a problem with the given target
					sys.exit(20)
				except http.client.HTTPException as e:
					logger.critical('%s HTTPException: %s', facility, repr(e))
					message = str(e)
					result = 'critical'
					yield {'_time': time.time(), 'app': app, 'collection': collection_name, 'records': posted, 'result': result, 'message': message }
					# exit because we could have a problem with the given target
					sys.exit(21)
			if result != 'skipped':
				# Collection now fully restored
				logger.info('%s Restored collection ' + collection_name + ' successfully.', facility)
				yield {'_time': time.time(), 'app': app, 'collection': collection_name, 'records': posted, 'result': result, 'message': message }

dispatch(KVStorePullCommand, sys.argv, sys.stdin, sys.stdout, __name__)
