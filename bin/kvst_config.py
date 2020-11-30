#!/usr/bin/env python

# Copyright 2020 Deductiv Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# REST Endpoint for configuration dashboard

# Author: J.R. Murray <jr.murray@deductiv.net>
# Version: 2.0.1

import sys
import os
import json
import re
from deductiv_helpers import setup_logger, eprint, str2bool

# Add lib folders to import path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'lib'))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib'))
# pylint: disable=import-error
import splunk.admin as admin
import splunk.rest as rest
import splunk.entity as en
from splunk.clilib import cli_common as cli
# https://github.com/HurricaneLabs/splunksecrets/blob/master/splunksecrets.py
from splunksecrets import encrypt, encrypt_new

options = ['log_level', 'default_path', 'backup_batch_size', 'compression', 'retention_days', 'retention_size']
for i in range(1, 20):
	options.append('credential' + str(i)) # credential1 through credential19

class ConfigApp(admin.MConfigHandler):

	# Set up supported arguments

	def setup(self):
		if self.requestedAction == admin.ACTION_EDIT:
			for arg in options:
				self.supportedArgs.addOptArg(arg)

	# Read default settings
	def handleList(self, confInfo):
		self.capabilityRead = 'read_kvst_config'

		try:
			cfg = cli.getConfStanza('kvstore_tools','settings')
		except BaseException as e:
			raise Exception("Could not read configuration: " + repr(e))
		
		# Facility info - prepended to log lines
		facility = os.path.basename(__file__)
		facility = os.path.splitext(facility)[0]
		try:
			logger = setup_logger(cfg["log_level"], 'kvstore_tools.log', facility)
		except BaseException as e:
			raise Exception("Could not create logger: " + repr(e))

		logger.debug('KV Store Tools Settings handler started (List)')
		
		# Check for permissions to read the configuration
		session_key = self.getSessionKey()
		content = rest.simpleRequest('/services/authentication/current-context?output_mode=json', sessionKey=session_key, method='GET')[1]
		content = json.loads(content)
		current_user = content['entry'][0]['content']['username']
		current_user_capabilities = content['entry'][0]['content']['capabilities']
		if self.capabilityRead in current_user_capabilities:
			logger.debug("User %s is authorized" % current_user)

			confDict = self.readConf("kvstore_tools")
			if None != confDict:
				for stanza, settings in list(confDict.items()):
					for key, val in list(settings.items()):
						logger.debug("key: {0}, value: {1}".format(key, val))
						if key in ['compression']:
							if str2bool(val):
								val = '1'
							else:
								val = '0'
						'''
						if key in ['default_path'] and val in [None, '', 'unset']:
							val = os.path.join('$SPLUNK_HOME', 'etc', 'apps', 'kvstore_tools', 'backups')
							# Windows wildcard support (works with $ but this is more clear).
							if '\\' in val:
								val = val.replace('$SPLUNK_HOME', '%SPLUNK_HOME%')
						if key in ['backup_batch_size'] and val in [None, '']:
							val = '50000'
						if key in ['retention_days'] and val in [None, '']:
							val = '0'
						if key in ['retention_size'] and val in [None, '']:
							val = '0'
						'''
						confInfo[stanza].append(key, val)
		else:
			raise Exception("User %s is unauthorized. Has the read_kvst_config capability been granted?" % current_user)

	# Update settings once they are saved by the user
	def handleEdit(self, confInfo):
		self.capabilityWrite = 'write_kvst_config'

		try:
			cfg = cli.getConfStanza('kvstore_tools','settings')
		except BaseException as e:
			raise Exception("Could not read configuration: " + repr(e))
		
		# Facility info - prepended to log lines
		facility = os.path.basename(__file__)
		facility = os.path.splitext(facility)[0]
		try:
			logger = setup_logger(cfg["log_level"], 'kvstore_tools.log', facility)
		except BaseException as e:
			raise Exception("Could not create logger: " + repr(e))

		logger.debug('KV Store Tools Settings handler started (Edit)')

		# Check for permissions to read the configuration
		session_key = self.getSessionKey()
		content = rest.simpleRequest('/services/authentication/current-context?output_mode=json', sessionKey=session_key, method='GET')[1]
		content = json.loads(content)
		current_user = content['entry'][0]['content']['username']
		current_user_capabilities = content['entry'][0]['content']['capabilities']
		if self.capabilityWrite in current_user_capabilities:
			logger.debug("User %s is authorized" % current_user)

			# Read the splunk.secret file
			with open(os.path.join(os.getenv('SPLUNK_HOME'), 'etc', 'auth', 'splunk.secret'), 'r') as ssfh:
				splunk_secret = ssfh.readline()

			config = self.callerArgs.data
			new_config = {}
			for k, v in list(config.items()):
				if isinstance(v, list) and len(v) == 1:
					v = v[0]
				if v is None:
					logger.debug('Setting %s to blank' % k)
					new_config[k] = ''
				else:
					logger.debug('Setting %s to %s' % (k, v))
					if k[:10] == 'credential' and not '$7$' in v:
						logger.debug('Value has an unencrypted password. Encrypting.')
						# Split the value into alias/username/password
						hostname, username, password = v.split(':')
						try:
							v = hostname + ":" + username + ":" + encrypt_new(splunk_secret, password)
						except BaseException as e:
							logger.error("Error saving encrypted password for %s: %s" % (hostname, repr(e)))
							continue
							
					logger.debug('Encrypted')
					new_config[k] = v
					logger.debug('applied to configuration dict')
			try:
				if 'compression' in list(new_config.keys()):
					if str2bool(config['compression'][0]):
						new_config['compression'][0] = '1'
					else:
						new_config['compression'][0] = '0'
			
				if 'default_path' in list(new_config.keys()):
					if config['default_path'][0] in [None, '']:
						new_config['default_path'][0] = None
			
				if 'backup_batch_size' in list(new_config.keys()):
					if config['backup_batch_size'][0] in [None, '']:
						new_config['backup_batch_size'][0] = None
				
				logger.debug("Writing configuration")
			except BaseException as e:
				logger.critical("Error parsing configuration: %s" % repr(e))
			# Write the config stanza
			self.writeConf('kvstore_tools', 'settings', new_config)
		else:
			raise Exception("User %s is unauthorized. Has the write_kvst_config capability been granted?" % current_user)

# initialize the handler
admin.init(ConfigApp, admin.CONTEXT_NONE)
