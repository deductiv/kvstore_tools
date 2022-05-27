#!/usr/bin/env python
#!/usr/bin/env python

# KV Store Pull
# Enables the download of remote collections to a local SH instance on a per-app basis
# Pulls collections from a remote search head or SHC node to the local SH KV store

# Author: J.R. Murray <jr.murray@deductiv.net>
# Version: 2.0.8

from __future__ import print_function
from builtins import str
from future import standard_library
standard_library.install_aliases()
import sys
import os
import json
import urllib.error, urllib.parse
import kv_common as kv
from deductiv_helpers import setup_logger, eprint, is_ipv4
from splunk.clilib import cli_common as cli
import splunk.rest as rest

# Add lib folders to import path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib'))
import splunklib.client as client
from splunklib.searchcommands import \
	dispatch, GeneratingCommand, Configuration, Option, validators

@Configuration()
class KVStorePullCommand(GeneratingCommand):
	""" %(synopsis)

	##Syntax

	| kvstorepull app="app_name" collection="collection_name" global_scope="false" target="remotehost" targetport=8089

	##Description

	Download each collection from a remote search head/SHC node to the local KV Store

	"""

	app = Option(
		doc='''
			Syntax: app=<appname>
			Description: The app to download collections from''',
			require=False)

	global_scope = Option(
		doc='''
			Syntax: global_scope=[true|false]
			Description: Include all globally available collections''',
			require=False, validate=validators.Boolean())

	collection = Option(
		doc='''
			Syntax: collection=<collection_name>
			Description: The collection to download from the specified app''',
			require=False)

	append = Option(
		doc='''
			Syntax: append=[true|false]
			Description: Append to the existing results (true) or delete existing entries on the local host prior to the data pull (false)''',
			require=False, validate=validators.Boolean())

	target = Option(
		doc='''
			Syntax: target=<hostname>
			Description: The hostname to download from. Credentials must be given via setup.''',
			require=True)

	targetport = Option(
		doc='''
			Syntax: port=<Port>
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
		if 'run_kvstore_pull' in current_user_capabilities or 'run_kvst_all' in current_user_capabilities or current_user == 'splunk-system-user':
			logger.debug("User %s is authorized." % current_user)
		else:
			logger.error("User %s is unauthorized. Has the run_kvstore_pull capability been granted?" % current_user)
			yield({'Error': 'User %s is unauthorized. Has the run_kvstore_pull capability been granted?' % current_user })
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
				if self.target in list(credentials.keys()):
					hostname = self.target
				else:
					if '.' in self.target and not is_ipv4(self.target):
						hostname = self.target.split('.')[0]
					else:
						raise KeyError
				credential = credentials[hostname]
				
			except KeyError:
				logger.critical("Could not get password for %s: Record not found" % hostname)
				print("Could not get password for %s: Record not found" % hostname)
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
		remote_app_list = kv.get_server_apps(remote_uri, remote_session_key, self.app)
		remote_collection_list = kv.get_app_collections(remote_uri, remote_session_key, self.collection, self.app, remote_app_list, self.global_scope)
		logger.debug('Collections to pull: %s' % str(remote_collection_list))

		for remote_collection in remote_collection_list:
			# Extract the app and collection name from the array
			collection_app = remote_collection[0]
			collection_name = remote_collection[1]
			try:
				yield(kv.copy_collection(logger, remote_session_key, remote_uri, local_session_key, splunkd_uri, collection_app, collection_name, self.append))
			except BaseException as e:
				logger.critical('Failed to copy collections from %s to local KV store: %s' % (self.target, repr(e)), exc_info=True)
				yield({'Error': 'Failed to copy collections from %s to local KV store: %s' % (self.target, repr(e)) } )
				sys.exit(11)
			
dispatch(KVStorePullCommand, sys.argv, sys.stdin, sys.stdout, __name__)
