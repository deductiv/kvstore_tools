#!/usr/bin/env python

# REST endpoint for configuration via setup.xml

# Author: J.R. Murray <jr.murray@deductiv.net>
# Version: 1.4.1


import splunk.admin as admin
import splunk.entity as en
import logging

class ConfigApp(admin.MConfigHandler):

	# Set up supported arguments

	def setup(self):
		if self.requestedAction == admin.ACTION_EDIT:
			for arg in ['max_rows_per_query']:
				self.supportedArgs.addOptArg(arg)

	# Read default settings
	def handleList(self, confInfo):
		
		logger = logging.getLogger('kvst')
		logger.debug('KV Store Tools KVStore Settings handler started')
		
		confDict = self.readConf("limits")
		if None != confDict:
			for stanza, settings in list(confDict.items()):
				for key, val in list(settings.items()):
					logger.debug("key: {0}, value: {1}".format(key, val))
					if key in ['max_rows_per_query']:
						val = 50000
					#if key in ['max_documents_per_batch_save']:
					#	val = 1000
					confInfo[stanza].append(key, val)

	# Update settings once they are saved by the user
	def handleEdit(self, confInfo):
		name = self.callerArgs.id
		args = self.callerArgs
		
		#if int(self.callerArgs.data['max_rows_per_query'][0]) < 50000:
		#	self.callerArgs.data['max_rows_per_query'][0] = '50000'
		
		# Write the config stanza
		self.writeConf('limits', 'kvstore', self.callerArgs.data)
      
# initialize the handler
admin.init(ConfigApp, admin.CONTEXT_NONE)
