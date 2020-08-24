#!/usr/bin/env python

# Alert action to add to KV Store
# Enables regular users to leverage searches to easily add/remove functionality for lookups
# Pushes each result into the KV Store regardless of [Once/For Each Result] setting
 
# Author: J.R. Murray <jr.murray@deductiv.net>
# Version: 1.4.1

from __future__ import print_function
from future import standard_library
standard_library.install_aliases()
import sys
import os
import json
import http.client, urllib.request, urllib.parse, urllib.error
import urllib.request, urllib.error, urllib.parse
from datetime import datetime
import gzip
import csv
import re

# Examples:
# http://docs.splunk.com/Documentation/Splunk/6.5.0/AdvancedDev/CustomAlertKVStoreExample


def request(method, url, data, headers):
	"""Helper function to fetch JSON data from the given URL"""
	req = urllib.request.Request(url, data, headers)
	req.get_method = lambda: method
	res = urllib.request.urlopen(req)
	res_txt = res.read()
	if len(res_txt)>0:
		return json.loads(res_txt)
	else:
		return ''

def str2bool(v):
	return v.lower() in ("yes", "true", "t", "1")

if sys.argv[1] == "--execute":
	
	payload = json.loads(sys.stdin.read())
	
	print("INFO Payload: %s" % json.dumps(payload), file=sys.stderr)
	#params = payload.get('configuration')
	
	# Get the payload
	config = payload.get('configuration', dict())
	# Get the collection name supplied by the user/search
	collection = config.get('collection')
	
	# Build the URL for the Splunkd REST endpoint
	url_tmpl_batch = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/data/%(collection)s/batch_save?output_mode=json'
	record_url = url_tmpl_batch % dict(
		server_uri=payload.get('server_uri'),
		owner='nobody',
		app=urllib.parse.quote(config.get('app') if 'app' in config else payload.get('app')),
		collection=collection)
	
	url_tmpl = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/data/%(collection)s/?output_mode=json'
	delete_url = url_tmpl % dict(
		server_uri=payload.get('server_uri'),
		owner='nobody',
		app=urllib.parse.quote(config.get('app') if 'app' in config else payload.get('app')),
		collection=collection)
	print('DEBUG Built kvstore record url=%s' % record_url, file=sys.stderr)
	
	# Build the HTTP header (including authorization string)
	headers = {
		'Authorization': 'Splunk %s' % payload.get('session_key'),
		'Content-Type': 'application/json'}
		
	
	# Get the data from the search results
	# Get event filename
	results_file = payload.get('results_file')
	#####
	if os.path.exists(results_file):
		reader = csv.reader(gzip.open(results_file))
		# Initialize rownum variable
		rownum = 0
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
				#print >>sys.stderr, 'INFO Columns from results: %s' % (str(column_names))
			else:
				j = {}
				for idx, column in enumerate(row):
					if not idx in excluded_columns and len(column) > 0:
						if '__mv_' in column_names[idx]:
							#print >>sys.stderr, 'INFO MV column %s with results: %s' % (column_names[idx], column)
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
		
	# Change the action if the overwrite flag is specified
	if str2bool(config.get('overwrite')):
		print('INFO Overwriting kvstore collection=%s with data=%s' % (collection, json.dumps(json_results)), file=sys.stderr)
		# Delete the collection contents
		try:
			response = request('DELETE', delete_url, "", headers)
			print('DEBUG server response for collection deletion:', json.dumps(response), file=sys.stderr)
		except urllib.error.HTTPError as e:
			print('ERROR Failed to delete collection:', json.dumps(json.loads(e.read())), file=sys.stderr)
			sys.exit(3)
	else:
		print('INFO Updating kvstore collection=%s with data=%s' % (collection, json.dumps(json_results)), file=sys.stderr)
	
	# Send the updated record to the server
	try:
		response = request('POST', record_url, json.dumps(json_results), headers)
		print('DEBUG server response:', json.dumps(response), file=sys.stderr)
	except urllib.error.HTTPError as e:
		print('ERROR Failed to update record:', json.dumps(json.loads(e.read())), file=sys.stderr)
		sys.exit(3)

