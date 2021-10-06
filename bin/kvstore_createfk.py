#!/usr/bin/env python

# KV store record creation - streaming search command
# Creates records in a collection based on group-by field
# Appends all events with the key value in a user-specified field,
#  which can be written to a second collection (referencing the keys in the first)

# Author: J.R. Murray <jr.murray@deductiv.net>
# Version: 2.0.4

from future import standard_library
from io import open
standard_library.install_aliases()
from builtins import str
import sys
import os
import json
import time
import re
import fcntl
import kv_common as kv
from deductiv_helpers import setup_logger, eprint

# Add lib folders to import path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'lib'))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib'))
# pylint: disable=import-error
from splunk.clilib import cli_common as cli
from splunklib.searchcommands import \
    dispatch, StreamingCommand, Configuration, Option, validators
from splunklib.client import connect

@Configuration(local=True)
class kvstore_createfkCommand(StreamingCommand):
	""" %(synopsis)
	
	##Syntax (Example)  

	search <events> | kvstorecreatefk collection=<collection1_name> outputkeyfield=<key_field_name> outputvalues="" | outputlookup append=t <collection2_name>

	##Description  

	Creates a single record in the target collection and appends the resulting key value to each event  

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
			Default: [collection]_key   ''',
			require=False)

	outputvalues = Option(
		doc='''
			Syntax: outputvalues=<kvpairs>
			Description: Specify the output fields/values to write to the new collection record. 
				Variable values use the first non-null field value in the search results.
			Example: outputvalues="kvfield1=Web Server, kvtimestamp=2020-01-01, kvstatus=$http_status$"  
			Default: None
			Special keywords: 
				$kv_now$ = Current timestamp in epoch format
				$kv_current_userid$ = Current Splunk user ID''',
			require=False)
			
	groupby = Option(
		doc='''
			Syntax: groupby=<field_name>","
			Description: Specify the field to group records by.  Creates a unique _key record for each distinct value.
			Default: None ''',
			require=False)
	delimiter = Option(
		doc='''
			Syntax: delimiter=","
			Description: Specify the delimiter for key-value pair strings. 
			Default: Comma (,) ''',
			require=False)


	def stream(self, events):
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
		
		if self.app:
			logger.debug('App: %s' % self.app)
		else:
			self.app = self._metadata.searchinfo.app

		if self.collection:
			logger.debug('Collection: %s' % self.collection)
		else:
			logger.critical("No collection specified. Exiting.")
			print("Error: No collection specified.")
			exit(1)

		if self.outputkeyfield:
			logger.debug('Output Key Field: %s' % self.outputkeyfield)
		else:
			self.outputkeyfield = self.collection + "_key"

		if self.outputvalues:
			logger.debug('Output Values: %s' % self.outputvalues)
		else:
			self.outputvalues = ""
		
		if self.delimiter:
			logger.debug('Delimiter: %s' % self.delimiter)
		else:
			self.delimiter = ","

		if self.groupby:
			logger.debug('Group by field: %s' % self.groupby)
		else:
			self.groupby = None


		opts = {}
		opts["owner"] = "nobody"
		opts["token"] = self._metadata.searchinfo.session_key
		opts["app"] = self.app

		#epoch_time = int(time.time())
		current_user = self._metadata.searchinfo.username

		lookup_output_kvpairs = []
		
		# Static output fields are literal values that are given within the search command arguments
		# e.g. "lookup_field1=value1"
		static_output_fields = {}
		# variable output fields are values taken from the events and pushed into the lookup record 
		# as events are processed
		# e.g. "lookup_field2=$sourcetype$"
		variable_output_fields = {}
		resolved_variables = {}

		# Check for lockfile from previous invocations for this search ID
		dispatch = self._metadata.searchinfo.dispatch_dir
		static_kvfields_file = os.path.join(dispatch, "kvfields_static")		#dict
		variable_kvfields_file = os.path.join(dispatch, "kvfields_variable")	#dict
		resolved_variables_file = os.path.join(dispatch, "resolved_variables")	#dict
		
		try:
			if os.path.isfile(static_kvfields_file):
				with open(static_kvfields_file, 'r') as f:
					# Set static kvfields values
					static_output_fields = json.loads(f.read())		#dict
			if os.path.isfile(variable_kvfields_file):
				with open(variable_kvfields_file, 'r') as f:
					# Set variable kvfields values
					variable_output_fields = json.loads(f.read())	#dict
			
			# Connect to the kv store
			service = connect(**opts)
			if self.collection in service.kvstore:
				obj_collection = service.kvstore[self.collection]
			else:
				logger.critical("KVStore not found: %s" % self.collection)
				print('KVStore not found: %s' % self.collection)
				exit(1)
			
			# First invocation - build the lists for static and variable values
			if static_output_fields == {} and variable_output_fields == {}:	
				
				# Split the key-value pairs argument into individual key-value pairs
				# Account for quoted string values and delimiters within the quoted value
				kvpair_split_re = r'([^=]+=(?:"[^"\\]*(?:\\.[^"\\]*)*"|[^{}]+))'.format(self.delimiter)
				x = re.findall(kvpair_split_re, self.outputvalues)
				for i in x:
					i = i.strip(self.delimiter).strip()
					lookup_output_kvpairs.append(i)

				for lof in lookup_output_kvpairs:
					k, v = lof.split("=")
					k = k.strip()
					v = v.strip().strip('"').replace('\\"', '"')
					logger.debug("k = %s, v = %s" % (k,v))

					# Replace special values
					v = v.replace("$kv_current_userid$", current_user)
					v = v.replace("$kv_now$", str(time.time()))

					# Value starts and ends with $ - variable field
					if v[0] + v[-1] == '$$':
						# Add to the list of variable fields
						variable_output_fields[k] = v.replace("$", "")
					else:
						# Add to the list of static fields
						static_output_fields[k] = v
				logger.info("Unpacked %d static and %d variable fields from arguments" % (len(list(static_output_fields.keys())), len(list(variable_output_fields.keys()))))

				# Write the static payload to the file
				# File doesn't exist. Open/claim it.
				with open(static_kvfields_file, 'w') as f:
					f.write(json.dumps(static_output_fields, ensure_ascii=False))
				with open(variable_kvfields_file, 'w') as f:
					f.write(json.dumps(variable_output_fields, ensure_ascii=False))
					
		except BaseException as e:
			logger.critical('Error connecting to collection: %s' % repr(e), exc_info=True)
			print('Error connecting to collection: %s' % repr(e))
			exit(1)


		# Read the events, resolve the variables, store them on a per-groupby-fieldvalue basis
		i = 0
		inserts = 0
		for e in events:
			update = False
			# (Re)read the latest data 
			if os.path.isfile(resolved_variables_file):
				with open(resolved_variables_file, 'r') as f:
					# Open in non-blocking mode
					fd = f.fileno()
					flag = fcntl.fcntl(fd, fcntl.F_GETFL)
					fcntl.fcntl(fd, fcntl.F_SETFL, flag | os.O_NONBLOCK)
					# Set static kvfields values
					resolved_variables = json.loads(f.read())	#dict [groupby value][field name]
			if self.groupby is not None:
				groupby_value = e[self.groupby]
			else:
				# Make this value the same for every event (no group-by)
				groupby_value = '____placeholder'
			
			new_kv_record = {}
			if groupby_value in list(resolved_variables.keys()):
				# Set the previously recorded key value for this group-by value within the event
				kvstore_entry_key = resolved_variables[groupby_value]["_key"]

				# We've already resolved the variables for this groupby, but see if any are not populated
				for lookup_field, event_field in list(variable_output_fields.items()):
					if lookup_field not in list(resolved_variables[groupby_value].keys()):
						if event_field in list(e.keys()):
							if e[event_field] is not None and e[event_field] != '':
								resolved_variables[groupby_value][lookup_field] = e[event_field]
								new_kv_record[lookup_field] = e[event_field]
								update = True
				if update:
					# Update the collection
					new_kv_record.update(static_output_fields)
					response = obj_collection.data.update(kvstore_entry_key, json.dumps(new_kv_record))

					# Write the data to disk immediately so other threads can benefit
					with open(resolved_variables_file, 'w') as f:
						f.write(json.dumps(resolved_variables, ensure_ascii=False))

			else:
				# First time we're seeing this groupby value. Resolve variables and write the KV store record.
				# Define the dictionary
				resolved_variables[groupby_value] = {}
				# Update the static values
				new_kv_record = static_output_fields.copy()

				# Resolve the variables
				for lookup_field, event_field in list(variable_output_fields.items()):
					if event_field in list(e.keys()):
						if e[event_field] is not None:
							resolved_variables[groupby_value][lookup_field] = e[event_field]
							new_kv_record[lookup_field] = e[event_field]
				
				# Write the new kvstore record and get the ID (_key) 
				response = obj_collection.data.insert(json.dumps(new_kv_record))
				kvstore_entry_key = response["_key"]
				resolved_variables[groupby_value]["_key"] = kvstore_entry_key

				# Write the data to disk immediately so other threads can benefit
				with open(resolved_variables_file, 'w') as f:
					f.write(json.dumps(resolved_variables, ensure_ascii=False))
					inserts += 1
			
			# Write the KV store record's _key value to the event
			e[self.outputkeyfield] = kvstore_entry_key

			yield e
			i += 1
		logger.info("Modified %d events and inserted %s new records into %s" % (i, inserts, self.collection))
	
dispatch(kvstore_createfkCommand, sys.argv, sys.stdin, sys.stdout, __name__)
