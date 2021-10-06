#!/usr/bin/env python

# Alert action to add to KV Store
# Enables regular users to leverage searches to easily add/remove functionality for lookups
# Pushes each result into the KV Store regardless of [Once/For Each Result] setting

# Author: J.R. Murray <jr.murray@deductiv.net>
# Version: 2.0.4

from __future__ import print_function
from future import standard_library
standard_library.install_aliases()
import sys
import os
import json
import urllib.error, urllib.parse
import gzip
import csv
import re
import kv_common as kv
from deductiv_helpers import eprint, request, str2bool, setup_logger, read_config

# Examples:
# http://docs.splunk.com/Documentation/Splunk/6.5.0/AdvancedDev/CustomAlertKVStoreExample

print(sys.argv)
if len(sys.argv) > 1:
	if sys.argv[1] == "--execute":
		payload = json.loads(sys.stdin.read())
		
		# Build the logger object based on the config file setting for log_level
		config = read_config('kvstore_tools.conf')
		log_level = config['settings']['log_level']
		facility = os.path.splitext(os.path.basename(__file__))[0]
		logger = setup_logger(log_level, 'kvstore_tools.log', facility)
		
		# Get the stdin payload
		alert_config = payload.get('configuration', dict())
		# Get the app / collection name supplied by the user/search
		app = urllib.parse.quote(alert_config.get('app') if 'app' in alert_config else payload.get('app'))
		collection = alert_config.get('collection')
		
		# Build the URL for the Splunkd REST endpoint
		url_tmpl_batch = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/data/%(collection)s/batch_save?output_mode=json'
		record_url = url_tmpl_batch % dict(
			server_uri = payload.get('server_uri'),
			owner = 'nobody',
			app = urllib.parse.quote(alert_config.get('app') if 'app' in alert_config else payload.get('app')),
			collection = collection)
		logger.debug('Built kvstore record url=%s' % record_url)

		# Build the HTTP header (including authorization string)
		headers = {
			'Authorization': 'Splunk %s' % payload.get('session_key'),
			'Content-Type': 'application/json'}
			
		# Get the data from the search results
		# Get event filename
		results_file = payload.get('results_file')
		logger.debug("Results file = %s" % results_file)

		# Initialize rownum variable
		rownum = 0
		if os.path.exists(results_file):
			try:
				reader = csv.reader(gzip.open(results_file, 'rt'))
				excluded_columns = []
				column_names = [None]*1000
				json_results = []
				for row in reader:
					if rownum == 0:
						for idx, column in enumerate(row):
							# Make a list of all of the column names
							column_names[idx] = column
							# Exclude special field names, but allow multivalue ones
							if column[0:2] == '__' and '__mv_' not in column:
								excluded_columns.append(idx)
						#logger.debug('Columns from results: %s' % (str(column_names)))
					else:
						j = {}
						for idx, column in enumerate(row):
							if not idx in excluded_columns and len(column) > 0:
								if '__mv_' in column_names[idx]:
									logger.debug('MV column %s with results: %s' % (column_names[idx], column))
									values = []
									# Results look like: $value1$;$value2$;$value3$
									for val in column.split(";"):
										try:
											if val != '$$':
												matches = re.match(r'\$(.+)\$', val)
												values.append(matches.group(1))
										except:
											continue
									j[column_names[idx][5:]] = values
								else:
									# Make a JSON object/dict with all of the row data
									j[column_names[idx]] = column
								# Add the result to the results array
						json_results.append(j)
					# Increment row count
					rownum += 1
			except BaseException as e:
				logger.error("Could not read or parse the results file", exc_info=True)
				exit(1)
		logger.info("Read %d results from results file" % rownum)

		# Change the action if the overwrite flag is specified
		if str2bool(alert_config.get('overwrite')):
			logger.debug('Overwriting kvstore collection=%s with data=%s' % (collection, json.dumps(json_results)))
			# Delete the collection contents
			try:
				response = kv.delete_collection(logger, payload.get('server_uri'), payload.get('session_key'), app, collection)
				logger.debug('Server response for collection deletion: %s' % response)
			except BaseException as e:
				logger.error('Failed to delete collection: %s' % repr(e))
				sys.exit(3)
		else:
			logger.debug('Updating kvstore collection=%s with data=%s' % (collection, json.dumps(json_results)))
		
		# Send the updated record to the server
		try:
			response, response_code = request('POST', record_url, json.dumps(json_results), headers)
			logger.debug('Server response: %s' % str(response))
			if response_code == 200:
				logger.info("Uploaded results to collection %s/%s successfully" % (app, collection))
		except BaseException as e:
			logger.error('Failed to update record: %s' % repr(e))
			sys.exit(3)

