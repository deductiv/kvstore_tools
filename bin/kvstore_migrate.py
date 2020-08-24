#!/usr/bin/env python

# KV Store Migrate
# Enables migration of collections on a per-app basis
# Pushes collections from a search head to a remote search head or SHC node

# Author: Florian Miehe
# Modified by: J.R. Murray <jr.murray@deductiv.net>
# Version: 1.4.1

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


@Configuration()
class KVStoreMigrateCommand(GeneratingCommand):
	""" %(synopsis)

	##Syntax

	| kvstoremigrate app="app_name" collection="collection_name" global_scope="false" target="remotehost"

	##Description

	migrate each collection in the KV Store to a JSON file in the path specified

	"""

	app = Option(
		doc='''
			Syntax: app=<appname>
			Description: Specify the app to backup collections from''',
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
			Description: Specify the hostname to migrate to. Credentials must be given via setup.''',
			require=True)

	targetport = Option(
		doc='''
			Syntax: port=<Port>
			Description: Specify the Splunk serviceport''',
			require=False, validate=validators.Integer(minimum=1025,maximum=65535))

	def request(self, method, url, data=None, headers=None):
		"""Helper function to fetch JSON data from the given URL"""
		#logger = logging.getLogger('kvst')
		if data is not None:
			try:
				data = urllib.parse.urlencode(data).encode("utf-8")
			except:
				data = data.encode("utf-8")

		#req = urllib.request.Request(url, data, headers)
		if headers is None:
			req = urllib.request.Request(url, data=data, method=method)
		else:
			req = urllib.request.Request(url, data=data, headers=headers, method=method)

		with urllib.request.urlopen(req) as res:
			res_txt = res.read().decode('utf-8')
			#logger.debug(res_txt)
			res_code = res.getcode()
			#logger.debug(res_code)
			if len(res_txt)>0:
				return json.loads(res_txt)
			else:
				return res_code

	# access the credentials in /servicesNS/nobody/app_name/admin/passwords
	def getCredentials(self, sessionKey):
		logger = logging.getLogger('kvst')
		myapp = 'kvstore_tools'
		try:
		# list all credentials
			entities = entity.getEntities(['admin', 'passwords'], namespace=myapp, owner='nobody', sessionKey=sessionKey)
		except Exception as e:
			raise Exception("Could not get %s credentials from Splunk. Error: %s" % (myapp, str(e)))

		creds = []
		# Enumerate credentials for this app
		for i, f in list(entities.items()):
			if f['eai:acl']['app'] == myapp:
				logger.debug(f)
				cred = [ f['username'], f['clear_password']]
				creds.append(cred)
		# Return last set of credentials
		return creds[len(creds)-1]

	def generate(self):
		logger = logging.getLogger('kvst')

		# Facility Info in the loglines
		facility = os.path.basename(__file__)
		facility = os.path.splitext(facility)[0]
		facility = facility.replace('_','')
		facility = "[%s]" % (facility)

		logger.debug('%s KVStoreMigrateCommand: %s', facility, self)
		errors = []

		# get service object for more infos about this session
		service = client.connect(token=self._metadata.searchinfo.session_key)

		# Check permissions
		required_role = "kv_admin"
		active_user = self._metadata.searchinfo.username
		if active_user in roles.get_role_users(self._metadata.searchinfo.session_key, required_role) or active_user == "admin":
			logger.debug("%s User %s is authorized.", facility, active_user)
		else:
			logger.error("%s User %s is unauthorized. Has the kv_admin role been granted?", facility, active_user)
			yield({'Error': 'User %s is unauthorized. Has the kv_admin role been granted?' % active_user })
			sys.exit(3)

		logger.info('%s kvstoremigrate started', facility)

		try:
			cfg = cli.getConfStanza('kvstore_tools','backups')
			limits_cfg = cli.getConfStanza('limits','kvstore')
		except BaseException as e:
			logger.error("%s ERROR getting configuration: " + str(e), facility)

		batch_size = int(cfg.get('backup_batch_size'))
		logger.debug("%s Batch size: %d rows", facility, batch_size)
		session_key = self._metadata.searchinfo.session_key
		splunkd_uri = self._metadata.searchinfo.splunkd_uri

		#if len(session_key) == 0:
		#	print("Did not receive a session key from splunkd. " +
		#					"Please enable passAuth in inputs.conf for this " +
		#					"script\n")
		#	sys.exit(2)

		# get credentials - might exit if no creds are available
		try:
			username, password = self.getCredentials(session_key)
			logger.debug("Username: %s / Password: %s", username, password)
		except BaseException as e:
			logger.critical('%s ERROR Failed to get credentials for remote Splunk instance: %s', facility, str(e))
			yield({'Error': 'Failed to get credentials for remote Splunk instance: ' + str(e)})
			
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

		url_tmpl_app = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/config?output_mode=json&count=0'

		# Login Remote and get the Remote session key
		try:
			remote_host = self.target
			remote_port = self.targetport
			remote_user = username
			remote_password = password
			remote_uri = 'https://' + self.target + ':' + str(self.targetport)
			
			remote_service = client.connect(
				host=remote_host,
				port=remote_port,
				username=remote_user,
				password=remote_password)
			remote_service.login()

			#remote_session_key = remote_service.SessionKey
			remote_session_key = remote_service.token
			logger.debug(remote_session_key)
			logger.debug('%s Remote Session_key: ' + remote_session_key, facility)
			#remote_creds = { 'username': username, 'password': password }
			#remote_creds = str(json.dumps(remote_creds))
			#remote_url = remote_uri + '/servicesNS/admin/search/auth/login'

			#server_content = self.request('POST', remote_url, remote_creds)
			
		except (urllib.error.HTTPError, BaseException) as e:
			logger.exception('%s ERROR Failed to login on remote Splunk instance: %s', facility, str(e))
			yield({'Error': 'Failed to login on remote Splunk instance: ' + str(e)})
			sys.exit(4)

		apps = []
		
		if self.app is not None:
			apps.append(self.app)
		else:
			# Enumerate all apps
			try:
				response, content = rest.simpleRequest("apps/local?output_mode=json", sessionKey=session_key, method='GET')
				#logger.debug('%s Server response: %s', facility, response)
				#logger.debug('%s Server content: %s', facility, content)
				content = json.loads(content)
				for entry in content["entry"]:
					if not entry["content"]["disabled"]:
						apps.append(entry["name"])

			except urllib.error.HTTPError as e:
				logger.critical('%s ERROR Failed to create app list: %s', json.dumps(json.loads(e.read())), facility)
				yield({ 'Error': 'Failed to create app list: %s' % json.dumps(json.loads(e.read())) })
				sys.exit(5)
			except urllib.error.URLError as e:
				logger.critical('%s ERROR URLError = %s', facility, repr(e) )
				yield({ 'Error': repr(e) })
				sys.exit(6)
			except http.client.HTTPException as e:
				logger.critical('%s HTTPException: %s', facility, repr(e))
				yield({ 'Error': repr(e) })
				sys.exit(7)
		collections = []
		for app in apps:
			logger.debug("%s Polling collections in app: %s" , facility, app)
			# Enumerate all of the collections in the app (if an app is selected)
			collections_url = url_tmpl_app % dict(
				server_uri=splunkd_uri,
				owner='nobody',
				app=app)
			headers = {
				'Authorization': 'Splunk %s' % session_key,
				'Content-Type': 'application/json'}
			try:
				response = self.request('GET', collections_url, '', headers)
			except urllib.error.HTTPError as e:
				logger.critical('%s ERROR Failed to download collection list: %s', facility, json.dumps(json.loads(e.read())))
				yield({ 'Error': 'Failed to download collection list: %s' % json.dumps(json.loads(e.read())) })
				sys.exit(8)
			except urllib.error.URLError as e:
				logger.critical('%s ERROR URLError = %s', facility, repr(e))
				yield({ 'Error': 'URL error: %s' % repr(e) })
				sys.exit(9)
			except http.client.HTTPException as e:
				logger.critical('%s HTTPException: %s', facility, repr(e))
				yield({ 'Error': 'HTTP Exception: %s' % repr(e) })
				sys.exit(10)

			logger.debug("%s Parsing response for collections in app: %s" , facility, app)
			for entry in response["entry"]:
				entry_app = entry["acl"]["app"]
				collection_name = entry["name"]
				#logger.debug(entry_app)
				#logger.debug(collection_name)
				sharing = entry["acl"]["sharing"]

				#logger.debug("Parsing entry: %s" % str(entry))

				if (self.app == entry_app and self.collection == collection_name) or (self.app is None and self.collection == collection_name) or (self.app == entry_app and self.collection is None) or (sharing == 'global' and self.global_scope) or (self.app is None and self.collection is None):

					c = [entry_app, collection_name]
					if c not in collections:
						collections.append(c)
					logger.debug("%s Added {0}/{1} to migration list".format(entry_app, collection_name), facility)

		logger.debug('%s Collections to migrate: %s', facility, str(collections))

		url_tmpl_collection = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/data/%(collection)s?limit=%(limit)s&skip=%(skip)s&output_mode=json'

		for collection in collections:
			# Reset every iteration to local splunk uri
			headers = {
				'Authorization': 'Splunk %s' % session_key,
				'Content-Type': 'application/json'}
			batched_response = ''

			# Extract the app and collection name from the array
			entry_app = collection[0]
			collection_name = collection[1]
			loop_record_count = None
			total_record_count = 0
			message = None
			maxrows = int(limits_cfg.get('max_rows_per_query'))
			logger.debug('%s Collection: %s', facility, collection)

			try:
				cursor = 0

				# If the loop record count is equal to batch size, we hit the limit. Keep going.
				while (loop_record_count is None or loop_record_count == batch_size):

					# Build the URL
					data_url = url_tmpl_collection % dict(
						server_uri=splunkd_uri,
						owner='nobody',
						app=entry_app,
						collection=collection_name,
						limit = batch_size,
						skip = cursor)

					# Download the data from the collection
					response = self.request('GET', data_url, '', headers)

					# Remove the first and last characters ( [ and ] )
					response = json.dumps(response)[1:-1]
					#logger.debug('Response: %s ' , response)
					loop_record_count = response.count('_key')
					total_record_count += loop_record_count
					logger.debug('%s We counted ' + str(total_record_count) + ' total records and ' + str(loop_record_count) + ' in this loop.', facility)

					# Append the records to the variable
					if loop_record_count > 0:
						## Write the leading [ or comma delimiter (between batches)
						if cursor == 0:
							batched_response = batched_response + '['
						else:
							batched_response = batched_response + ','
						batched_response = batched_response + response
						if loop_record_count < batch_size:
							batched_response = batched_response + ']'
					cursor += loop_record_count

				logger.debug("%s Retrieved {0} records from {1}".format(total_record_count, collection_name), facility)

				if total_record_count > 0:
					logger.debug('%s maxrows per query: ' + str(maxrows))
					if total_record_count == maxrows:
						logger.warning('%s Stored up KV store collection up to the limit: %s/%s', facility, entry_app, collection_name)
						result = "warning"
						message = "Rows returned equal to configured limit. Possible incomplete backup."
					if batch_size > maxrows and total_record_count > maxrows:
						logger.warning('%s Stored up KV store collection with batches exceeding the limit: %s/%s', facility, entry_app, collection_name)
						result = "warning"
						message = "Batch size greater than configured query limit. Possible incomplete backup."
					else:
						logger.info('%s Stored up KV store collection successfully: %s/%s', facility, entry_app, collection_name)
						result = "success"
						message = "downloaded collection"
					# make it a json object
					batched_response = json.loads(batched_response)
				else:
					logger.debug('Skipping collection: ' + collection_name)
					result = "skipped"
					message = "collection is empty"

			except BaseException as e:
				logger.critical('%s ERROR Failed to download collection: %s', facility, str(e))
				#logger.debug(str(headers))
				result = "error"
				message = str(e)
				total_record_count = 0
				errors.append('%s ERROR downloading collection: ' + entry_app + '/' + collection_name, facility)

			yield {'_time': time.time(), 'app': entry_app, 'collection': collection_name, 'result': result, 'records': total_record_count, 'message': message, }
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
				app=app)

			# Build the URL for deleting the collection
			delete_url = url_tmpl % dict(
				server_uri=remote_uri,
				owner='nobody',
				app=app,
				collection=collection_name)

			# Build the URL for updating the collection
			record_url = url_tmpl_batch % dict(
				server_uri=remote_uri,
				owner='nobody',
				app=app,
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
				response = self.request('GET', collections_url, '', headers)
			except urllib.error.HTTPError as e:
				logger.critical('%s ERROR Failed to download remote collection list: %s', facility, e.read() )
				yield({'Error': 'Failed to download remote collection list: %s' % e.read() } )
				sys.exit(11)
			except urllib.error.URLError as e:
				logger.critical('%s ERROR URLError = %s', facility, repr(e))
				yield({'Error': 'URL Error: %s' % repr(e) })
				sys.exit(12)
			except http.client.HTTPException as e:
				logger.critical('%s HTTPException: %s', facility, repr(e))
				yield({'Error': 'HTTP Exception: %s' % repr(e) })
				sys.exit(13)

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

dispatch(KVStoreMigrateCommand, sys.argv, sys.stdin, sys.stdout, __name__)
