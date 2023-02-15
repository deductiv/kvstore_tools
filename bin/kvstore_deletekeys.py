#!/usr/bin/env python

# KV Store Collection Record Deleter
# Deletes records from a KV Store collection based on _key value in search results
# Parameters are based on search results

# Author: J.R. Murray <jr.murray@deductiv.net>
# Version: 2.0.9

from __future__ import print_function
from builtins import str
from future import standard_library
standard_library.install_aliases()
import sys
import os
import urllib.parse
import http.client as httplib
import kv_common as kv
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import threading
from deductiv_helpers import request, setup_logger, search_console
from splunk.clilib import cli_common as cli

# Add lib folders to import path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib'))
from splunklib.searchcommands import \
    dispatch, StreamingCommand, Configuration, Option

lock = threading.Lock()
cfg = cli.getConfStanza('kvstore_tools','settings')
# Facility info - prepended to log lines
facility = os.path.basename(__file__)
facility = os.path.splitext(facility)[0]
logger = setup_logger(cfg["log_level"], 'kvstore_tools.log', facility)

@Configuration(local=True)
class KVStoreDeleteKeysCommand(StreamingCommand):
	""" %(synopsis)

	##Syntax

	search | deletekeys app="app_name" collection="collection_name"

	##Description

	Deletes records from a KV Store collection based on _key value in search results

	"""

	app = Option(
		doc='''
		 Syntax: app=<appname>
		 Description: Specify the app that the collection belongs to''',
		require=False)

	collection = Option(
		doc='''
		 Syntax: collection=<collection_name>
		 Description: Specify the collection name''',
		require=True)

	key_field = Option(
		doc='''
		 Syntax: key_field=<field_name>
		 Description: Specify the field name from the event''',
		require=False)

	splunkd_uri = None
	session_key = None
	conn = None

	def delete_key_from_event(self, delete_event):
		url_tmpl_delete = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/data/%(collection)s/%(id)s?output_mode=json'
		headers = {
			'Authorization': 'Splunk %s' % self.session_key,
			'Content-Type': 'application/json'}
		try:
			event_dict = dict(delete_event)
			# Throws an error using the native OrderedDict if key doesn't exist
			# Use a copy instead
			if self.key_field in list(event_dict.keys()): 
				event_key_value = event_dict[self.key_field]
				if len(event_key_value) > 0:
					logger.debug("Found key (%s) in event" % event_key_value)
					try:
						delete_url = url_tmpl_delete % dict(
							server_uri = self.splunkd_uri,
							owner = 'nobody',
							app = self.app,
							collection = self.collection,
							id = urllib.parse.quote(event_key_value, safe=''))

						try:
							lock.acquire()
							response, response_code = request('DELETE', delete_url, '', headers, self.conn)
							logger.debug('Server response for key %s: %s' % (event_key_value, response))
							lock.release()
						except BaseException as e:
							logger.error('ERROR Failed to delete key %s: %s', (event_key_value, repr(e)))

						if response_code == 200:
							logger.debug("Successfully deleted key " + event_key_value)
							delete_event['delete_status'] = "success"
							return delete_event
						else:
							logger.error("Error %d deleting key %s: %s" % (response_code, event_key_value, response))
							delete_event['delete_status'] = "error"
							return delete_event
					except BaseException as e:
						logger.error("Error deleting key %s: %s" % (event_key_value, repr(e)))
						delete_event['delete_status'] = "error"
						return delete_event
			else:
				logger.error("Key field not found in event: %s", event_dict)
		except BaseException as e:
			logger.exception("Error processing event: %s", e)
			
	def stream(self, events):
		ui = search_console(logger, self)
		logger.info('Script started by %s' % self._metadata.searchinfo.username)

		if self.app:
			logger.debug('App: %s' % self.app)
		else:
			self.app = self._metadata.searchinfo.app

		if self.collection:
			logger.debug('Collection: %s' % self.collection)
		else:
			ui.exit_error("No collection specified. Exiting.")
		
		if self.key_field is None:
			self.key_field = "_key"
		
		self.session_key = self._metadata.searchinfo.session_key
		self.splunkd_uri = self._metadata.searchinfo.splunkd_uri
		splunkd_url_tuple = urllib.parse.urlparse(self.splunkd_uri)
		self.conn = httplib.HTTPSConnection(splunkd_url_tuple.netloc)

		# Enumerate all app_list
		app_list = kv.get_server_apps(self.splunkd_uri, self.session_key, self.app)
		collection_list = kv.get_app_collections(self.splunkd_uri, self.session_key, self.collection, self.app, app_list, True)
		
		logger.debug('Collections present: %s', str(collection_list))

		try:
			# Create an object for the collection
			collection_present = False
			for c in collection_list:
				# Extract the app and collection name from the array
				# c[0] = app, c[1] = collection name
				collection_app = c[0]
				collection_name = c[1]
				if (collection_name == self.collection):
					if self.app is None or self.app == collection_app:
						self.app = collection_app
						collection_present = True
					elif self.app != collection_app:
						pass
					logger.debug("Collection {0} found in app {1}".format(self.collection, self.app))
			if not collection_present:
				ui.exit_error("KVStore collection %s/%s not found" % (self.app, self.collection))

		except BaseException as e:
			ui.exit_error('Error enumerating collections: %s' % repr(e))

		# Make a Pool of workers
		pool = ThreadPool(4)

		try:
			results = pool.map(self.delete_key_from_event, events)
		except BaseException as e:
			logger.error("%s" % repr(e), exc_info=True)
			results = {}
			
		for result in results:
			yield result

dispatch(KVStoreDeleteKeysCommand, sys.argv, sys.stdin, sys.stdout, __name__)
