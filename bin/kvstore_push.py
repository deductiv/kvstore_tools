#!/usr/bin/env python

# KV Store Push
# Enables the upload of local collections to a remote SH instance on a per-app basis
# Pushes collections from a local search head to a remote SH or SHC node KV store

# Author: J.R. Murray <jr.murray@deductiv.net>
# Version: 2.0.1

from future import standard_library
standard_library.install_aliases()
from builtins import str
import sys
import os, stat
import json
import http.client, urllib.error, urllib.parse
import time
from datetime import datetime
import gzip
import glob
import shutil
import re
from xml.dom import minidom
import kv_common as kv
from deductiv_helpers import request, setup_logger, eprint

# Add lib folders to import path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'lib'))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib'))
# pylint: disable=import-error
from splunk.clilib import cli_common as cli
from splunklib.searchcommands import \
	dispatch, GeneratingCommand, Configuration, Option, validators
from splunk.clilib import cli_common as cli
import splunklib.client as client
import splunk.rest as rest
import splunk.entity as entity

@Configuration()
class KVStorePushCommand(GeneratingCommand):
	""" %(synopsis)

	##Syntax  

	| kvstorepush app="app_name" collection="collection_name" global_scope="false" target="remotehost" targetport=8089  

	##Description  

	Upload each collection in the KV Store to a remote Splunk Search Head/SHC instance  

	"""

	app = Option(
		doc='''
			Syntax: app=<appname>
			Description: The app to select collections from''',
			require=False)

	global_scope = Option(
		doc='''
			Syntax: global_scope=[true|false]
			Description: Include all globally available collections''',
			require=False, validate=validators.Boolean())

	collection = Option(
		doc='''
			Syntax: collection=<collection_name>
			Description: The collection to push within the specified app''',
			require=False)

	append = Option(
		doc='''
			Syntax: append=[true|false]
			Description: Append to the existing results (true) or delete existing entries on the target prior to the data push (false)''',
			require=False, validate=validators.Boolean())

	target = Option(
		doc='''
			Syntax: target=<hostname>
			Description: The hostname to upload to. Credentials must be given via setup.''',
			require=True)

	targetport = Option(
		doc='''
			Syntax: targetport=<port>
			Description: Specify the Splunk REST API port''',
			require=False, validate=validators.Integer(minimum=1,maximum=65535))

	def generate(self):
		try:
			cfg = cli.getConfStanza('kvstore_tools','settings')
		except BaseException as e:
			eprint("Could not read configuration: " + repr(e))
		
		# Facility info - prepended to log lines
		facility = os.path.basename(__file__)
		facility = os.path.splitext(facility)[0]
		try:
			logger = setup_logger(cfg["log_level"], 'kvstore_tools.log', facility)
		except BaseException as e:
			eprint("Could not create logger: " + repr(e))
			print("Could not create logger: " + repr(e))
			exit(1)

		logger.info('Script started by %s' % self._metadata.searchinfo.username)

		batch_size = int(cfg.get('backup_batch_size'))
		logger.debug("Batch size: %d rows" % batch_size)

		local_session_key = self._metadata.searchinfo.session_key
		splunkd_uri = self._metadata.searchinfo.splunkd_uri

		# Check for permissions to run the command
		content = rest.simpleRequest('/services/authentication/current-context?output_mode=json', sessionKey=local_session_key, method='GET')[1]
		content = json.loads(content)
		current_user = self._metadata.searchinfo.username
		current_user_capabilities = content['entry'][0]['content']['capabilities']
		if 'run_kvstore_push' in current_user_capabilities or 'run_kvst_all' in current_user_capabilities:
			logger.debug("User %s is authorized." % current_user)
		else:
			logger.error("User %s is unauthorized. Has the run_kvstore_push capability been granted?" % current_user)
			yield({'Error': 'User %s is unauthorized. Has the run_kvstore_push capability been granted?' % current_user })
			sys.exit(3)
		
		# Sanitize input
		if self.app:
			logger.debug('App Context: %s' % self.app)
		else:
			self.app = None

		if self.collection:
			logger.debug('Collection: %s' % self.collection)
		else:
			self.collection = None

		if self.global_scope:
			logger.debug('Global Scope: %s' % self.global_scope)
		else:
			self.global_scope = False

		if self.append:
			logger.debug('Appending to existing collection')
		else:
			self.append = False
			logger.debug('Append to existing collection: %s' % str(self.append))

		if self.targetport:
			logger.debug('Port for remote connect: %s' % self.targetport)
		else:
			self.targetport = '8089'

		# Get credentials
		try:
			# Use the credential where the realm matches the target hostname
			# Otherwise, use the last entry in the list
			credentials = kv.parse_custom_credentials(logger, cfg)
			try:
				credential = credentials[self.target]
			except:
				try:
					hostname = self.target.split('.')[0]
					credential = credentials[hostname]
				except:
					logger.critical("Could not get password for %s: %s" % (self.target, repr(e)))
					print("Could not get password for %s: %s" % (self.target, repr(e)))
					exit(1593)
			
			remote_user = credential['username']
			remote_password = credential['password']
			
		except BaseException as e:
			logger.critical('Failed to get credentials for remote Splunk instance: %s' % repr(e), exc_info=True)
			yield({'Error': 'Failed to get credentials for remote Splunk instance: %s' % repr(e)})
			exit(7372)
		
		# Login to the remote host and get the session key
		try:
			remote_host = self.target
			remote_port = self.targetport
			remote_uri = 'https://%s:%s' % (self.target, self.targetport)
			
			remote_service = client.connect(
				host = remote_host,
				port = remote_port,
				username = remote_user,
				password = remote_password)
			remote_service.login()

			remote_session_key = remote_service.token.replace('Splunk ', '')
			logger.debug('Remote Session_key: %s' % remote_session_key)
			
		except (urllib.error.HTTPError, BaseException) as e:
			logger.exception('Failed to login on remote Splunk instance: %s' % repr(e))
			yield({'Error': 'Failed to login on remote Splunk instance: %s' % repr(e)})
			sys.exit(4424)

		# Get the list of remote apps and collections
		local_app_list = kv.get_server_apps(splunkd_uri, local_session_key, self.app)
		local_collection_list = kv.get_app_collections(splunkd_uri, local_session_key, self.collection, self.app, local_app_list, self.global_scope)
		logger.debug('Collections to push: %s' % str(local_collection_list))

		for local_collection in local_collection_list:
			# Extract the app and collection name from the array
			collection_app = local_collection[0]
			collection_name = local_collection[1]
			try:
				yield(kv.copy_collection(logger, local_session_key, splunkd_uri, remote_session_key, remote_uri, collection_app, collection_name, self.append))
			except BaseException as e:
				logger.critical('Failed to copy collections from %s to remote KV store: %s' % (self.target, repr(e)), exc_info=True)
				yield({'Error': 'Failed to copy collections from %s to remote KV store: %s' % (self.target, repr(e)) } )
				sys.exit(11)
			
dispatch(KVStorePushCommand, sys.argv, sys.stdin, sys.stdout, __name__)
