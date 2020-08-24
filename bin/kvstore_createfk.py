#!/usr/bin/env python

# KV store record creation - streaming search command
# Creates a single record in a collection
# Appends all events with the key value in a user-specified field,
#  which can be written to a second collection (referencing the keys in the first)

# Author: J.R. Murray <jr.murray@deductiv.net>
# Version: 1.4.1
# 2020-03-30

from future import standard_library
from io import open
standard_library.install_aliases()
from builtins import str
import sys
from splunk.clilib import cli_common as cli
from splunklib.searchcommands import \
    dispatch, StreamingCommand, Configuration, Option, validators
from splunklib.client import connect
import splunk
import os
import json
import http.client, urllib.request, urllib.parse, urllib.error
import time
#from datetime import datetime
#import gzip
#import shutil
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

def setup_logging():
	logger = logging.getLogger('intuit_af')
	return logger

@Configuration(local=True)
class kvstore_createfkCommand(StreamingCommand):
	""" %(synopsis)
	
	##Syntax

	search <events> | kvstorecreatefk collection=<collection1_name> outputkeyfield=<key_field_name> | outputlookup append=t <collection2_name>

	##Description

	Creates a single record in the target collection and appends the value to each event

	"""
	
	app = Option(
		doc='''
			Syntax: app=<appname>
			Description: Specify the app where the collections lives. 
			Default: Current app''',
			require=False)

	collection = Option(
		doc='''
			Syntax: collection=<collection_name>
			Description: Specify the collection to create the new record within.
			Default: None''',
			require=True)

	outputkeyfield = Option(
		doc='''
			Syntax: outputkeyfield=<field_name>
			Description: Specify the output field to write the new key value to. 
			Default: _key   ''',
			require=False)

	staticvalues = Option(
		doc='''
			Syntax: staticvalues=<kvpairs>
			Description: Specify the static fields/values to write to the collection record. 
			Example: staticvalues="kvfield1=Web Server, kvtimestamp=2020-01-01"  
			Default: None
			Special keywords: 
				kv_now = Current timestamp in epoch format
				kv_current_userid = Current Splunk user ID   ''',
			require=False)

	dynamicvalues = Option(
		doc='''
			Syntax: dynamicvalues=<kvpairs>
			Description: Specify the dynamic fields/values to write to the collection record. Uses the first non-null field value in the search results.
			Example: dynamicvalues="kvstatus=http_status, kvtimestamp=_time"  
			Default: None ''',
			require=False)

	delimiter = Option(
		doc='''
			Syntax: delimiter=","
			Description: Specify the delimiter for key-value pair strings. 
			Default: Comma (,) ''',
			require=False)


	def stream(self, events):
		logger = setup_logging()
		logger.debug('Script started')
		#logger.debug(sys.argv)
		#logger.debug(self._metadata)
		errors = []
		
		if self.app:
			logger.debug('App: %s', self.app)
		else:
			self.app=self._metadata.searchinfo.app

		if self.collection:
			logger.debug('Collection: %s', self.collection)
		else:
			self.collection=None
			logger.critical("No collection specified. Exiting.")
			exit(1)

		if self.outputkeyfield:
			logger.debug('Output Key Field: %s', self.outputkeyfield)
		else:
			self.outputkeyfield="_key"

		if self.staticvalues:
			logger.debug('Static Values: %s', self.staticvalues)
		else:
			self.staticvalues=""

		if self.dynamicvalues:
			logger.debug('Dynamic Values: %s', self.dynamicvalues)
		else:
			self.dynamicvalues=""

		if self.delimiter:
			logger.debug('Delimiter: %s', self.delimiter)
		else:
			self.delimiter=","


		opts = {}
		opts["owner"] = "nobody"
		opts["token"] = self._metadata.searchinfo.session_key
		opts["app"] = self.app

		epoch_time = int(time.time())
		current_user = self._metadata.searchinfo.username

		static_fields = []
		dynamic_fields = []

		try:
			# Check for lockfile from previous invocations for this search ID
			dispatch = self._metadata.searchinfo.dispatch_dir
			kvfields_file = os.path.join(dispatch, "kvfields")
			if os.path.isfile(kvfields_file):
				with open(kvfields_file, 'r') as f:
					kvfields = json.loads(f.read())
			else:
				# File doesn't exist. Open/claim it.
				with open(kvfields_file, 'w') as f:
					
					# Connect to the kv store
					service = connect(**opts)
					if self.collection in service.kvstore:
						obj_collection = service.kvstore[self.collection]
					else:
						logger.critical("KVStore not found: %s" % self.collection)
						exit(1)
					
					# Split the key-value pairs
					static_fields = self.split_fields(self.staticvalues, self.delimiter)

					kvfields = {}
					for sf in static_fields:
						k, v = sf.split("=")
						k = k.strip()
						v = v.strip()

						# Replace special values
						v = v.replace("kv_current_userid", current_user)
						v = v.replace("kv_now", str(time.time()))

						# Add to the list of fields
						kvfields[k] = v
					logger.info("Unpacked " + str(len(list(kvfields.keys()))) + " static fields from arguments")

					# Write the new kvstore record and get the ID (_key) 
					response = obj_collection.data.insert(json.dumps(kvfields))
					kvfields["_key"] = response["_key"]
					kvkey = kvfields["_key"]

					# Write the static payload to the file
					f.write(json.dumps(kvfields, ensure_ascii=False))
					
				logger.info("Created key ID %s" % kvkey)
			
		except BaseException as e:
			logger.critical('Error connecting to collection: ' + str(e))
			exit(1)

		if len(kvfields) > 0:
			dynamic_field_dict = {}
			if len(self.dynamicvalues) > 0:
				dynamic_fields = self.split_fields(self.dynamicvalues, self.delimiter)
				for df in dynamic_fields:
					k, v = df.split("=")
					k = k.strip()
					v = v.strip()
					if k not in list(kvfields.keys()):
						dynamic_field_dict[k] = v
				logger.info("Unpacked " + str(len(list(dynamic_field_dict.keys()))) + " dynamic fields from arguments")

			# Update all events with this KV Store ID
			i = 0
			# Create a copy for comparison later, so we can see if anything changed
			new_kvfields = kvfields.copy()
			for e in events:
				# Add the key value to the events
				e[self.outputkeyfield] = new_kvfields["_key"]

				# Read the configured fields from the event to copy into the kv store record
				# Only once per configured field
				for key, value in list(dynamic_field_dict.items()):
					# Break this apart to alleviate confusion
					kvstore_field = key
					event_field = value
					# If the event has the field and we haven't processed the field yet
					if event_field in list(e.keys()) and kvstore_field not in list(new_kvfields.keys()):
						# Don't copy the value if it's an empty string
						if e[event_field] != '':
							# Copy the value from the event to the kvfields dict
							new_kvfields[kvstore_field] = e[event_field]
							del dynamic_field_dict[key]
						logger.debug("New kvfields dict %s " % str(new_kvfields))

				yield e
				i += 1
			logger.info("Modified %d events with %s %s" % (i, self.outputkeyfield, kvfields["_key"]))
			
			# Update the KV Store with the updated kvfields payload
			if obj_collection is None:
				# Connect to the kv store
				service = connect(**opts)
				if self.collection in service.kvstore:
					obj_collection = service.kvstore[self.collection]
				else:
					logger.critical("KVStore not found to update after posting events: %s" % self.collection)
					exit(1)

			# Check if we have recorded any new values
			if new_kvfields != kvfields:
				# Update the collection
				response = obj_collection.data.update(new_kvfields["_key"], json.dumps(new_kvfields))
				logger.debug("Updated collection and received response: %s" % response)

		else:
			logger.critical("Error creating record. No key returned.")
			exit(1)

	def split_fields(self, fields_string, delimiter):
		fields = fields_string.split(delimiter)
		# Convert to array
		if not isinstance(fields, list):
			fields = [fields]
		return fields

dispatch(kvstore_createfkCommand, sys.argv, sys.stdin, sys.stdout, __name__)