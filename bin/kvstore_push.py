#!/usr/bin/env python

# KV Store Push
# Enables the upload of local collections to a remote SH instance on a per-app basis
# Pushes collections from a local search head to a remote SH or SHC node KV store

# Author: J.R. Murray <jr.murray@deductiv.net>
# Version: 2.0.9

from __future__ import print_function
from builtins import str
from future import standard_library
standard_library.install_aliases()
import sys
import os
import json
import urllib.error
import urllib.parse
import kv_common as kv
from deductiv_helpers import setup_logger, eprint, is_ipv4, search_console
from splunk.clilib import cli_common as cli
import splunk.rest as rest

# Add lib folders to import path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib'))
import splunklib.client as client
from splunklib.searchcommands import \
	dispatch, GeneratingCommand, Configuration, Option, validators

@Configuration(distributed=False, type='reporting')
class KVStorePushCommand(GeneratingCommand):
	""" %(synopsis)

	##Syntax  

	| kvstorepush app="app_name" collection="collection_name" global_scope="false" target="remotehost[, remotehost2, ...]" append=[true|false] targetport=8089  

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
			Syntax: target=<hostname1, hostname2, ...>
			Description: The list of hostnames to upload to. Credentials must be given via setup.''',
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
			self.write_error("Could not read configuration: " + repr(e))
			exit(1)
		
		# Facility info - prepended to log lines
		facility = os.path.basename(__file__)
		facility = os.path.splitext(facility)[0]
		logger = setup_logger(cfg["log_level"], 'kvstore_tools.log', facility)
		ui = search_console(logger, self)
		logger.info('Script started by %s' % self._metadata.searchinfo.username)

		batch_size = int(cfg.get('backup_batch_size'))
		logger.debug("Batch size: %d rows" % batch_size)

		local_session_key = self._metadata.searchinfo.session_key
		splunkd_uri = self._metadata.searchinfo.splunkd_uri

		# Check for permissions to run the command
		content = rest.simpleRequest('/services/authentication/current-context?output_mode=json', sessionKey=local_session_key)[1]
		content = json.loads(content)
		current_user = self._metadata.searchinfo.username
		current_user_capabilities = content['entry'][0]['content']['capabilities']
		if 'run_kvstore_push' in current_user_capabilities or 'run_kvst_all' in current_user_capabilities or current_user == 'splunk-system-user':
			logger.debug("User %s is authorized." % current_user)
		else:
			ui.exit_error("User %s is unauthorized. Has the run_kvstore_push capability been granted?" % current_user)
		
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

		#split target into list
		target_list = map(str.strip, self.target.split(','))

		for host in target_list:
			# Get credentials
			try:
				# Use the credential where the realm matches the host hostname
				# Otherwise, use the last entry in the list
				credentials = kv.parse_custom_credentials(logger, cfg)
				try:
					if self.target in list(credentials.keys()):
						hostname = self.target
					else:
						if '.' in self.target and not is_ipv4(self.target):
							hostname = self.target.split('.')[0]
						else:
							raise KeyError
					credential = credentials[hostname]
					remote_user = credential['username']
					remote_password = credential['password']
				
				except KeyError:
					ui.exit_error("Could not get password for %s: Record not found" % hostname)
			except BaseException as e:
				ui.exit_error('Failed to get credentials for remote Splunk instance: %s' % repr(e))
			
			# Login to the remote host and get the session key
			try:
				remote_host = host
				remote_port = self.targetport
				remote_uri = 'https://%s:%s' % (host, self.targetport)
				
				remote_service = client.connect(
					host = remote_host,
					port = remote_port,
					username = remote_user,
					password = remote_password)
				remote_service.login()

				remote_session_key = remote_service.token.replace('Splunk ', '')
				logger.debug('Remote session key: %s' % remote_session_key)
				
			except (urllib.error.HTTPError, BaseException) as e:
				ui.exit_error('Failed to login to remote Splunk instance: %s' % repr(e))

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
					ui.exit_error('Failed to copy collections from %s to remote KV store: %s' % (host, repr(e)))
			
dispatch(KVStorePushCommand, sys.argv, sys.stdin, sys.stdout, __name__)
