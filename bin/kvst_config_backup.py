#!/usr/bin/env python

# REST endpoint for configuration via setup.xml

# Author: J.R. Murray <jr.murray@deductiv.net>
# Version: 1.4.1


import splunk.admin as admin
import splunk.entity as en
import logging
import os
import re

def str2bool(v):
	return v.lower() in ("yes", "true", "t", "1")

class ConfigApp(admin.MConfigHandler):

	# Set up supported arguments

	def setup(self):
		if self.requestedAction == admin.ACTION_EDIT:
			for arg in ['default_path', 'backup_batch_size', 'compression', 'retention_days', 'retention_size']:
				self.supportedArgs.addOptArg(arg)

	# Read default settings
	def handleList(self, confInfo):
		
		logger = logging.getLogger('kvst')
		logger.debug('KV Store Tools Backup Settings handler started')
		
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
					confInfo[stanza].append(key, val)

	# Update settings once they are saved by the user
	def handleEdit(self, confInfo):
		name = self.callerArgs.id
		args = self.callerArgs

		if str2bool(self.callerArgs.data['compression'][0]):
			self.callerArgs.data['compression'][0] = '1'
		else:
			self.callerArgs.data['compression'][0] = '0'
    
		if self.callerArgs.data['default_path'][0] in [None, '']:
			self.callerArgs.data['default_path'][0] = ''
	
		if self.callerArgs.data['backup_batch_size'][0] in [None, '']:
			self.callerArgs.data['backup_batch_size'][0] = ''
		
		#if int(self.callerArgs.data['field_3'][0]) < 60:
		#	self.callerArgs.data['field_3'][0] = '60'
		
		# Write the config stanza
		self.writeConf('kvstore_tools', 'backups', self.callerArgs.data)
      
# initialize the handler
admin.init(ConfigApp, admin.CONTEXT_NONE)
