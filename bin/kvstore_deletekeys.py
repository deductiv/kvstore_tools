#!/usr/bin/env python

# KV Store Collection Record Deleter
# Deletes records from a KV Store collection based on _key value in search results
# Parameters are based on search results

# Author: J.R. Murray <jr.murray@deductiv.net>
# Version: 2.0.0

from future import standard_library
standard_library.install_aliases()
from builtins import str
import sys
import os
import json
import http.client, urllib.request, urllib.parse, urllib.error
import time
import kv_common as kv
from deductiv_helpers import request, setup_logger, eprint

# Multithreading
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

# Add lib folders to import path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'lib'))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib'))
# pylint: disable=import-error
from splunk.clilib import cli_common as cli
from splunklib.searchcommands import \
    dispatch, StreamingCommand, Configuration, Option, validators
from splunklib.client import connect
import splunk.rest as rest
import splunk

# sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

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

	splunkd_uri = None
	session_key = None

	def delete_key_from_event(self, delete_event):
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

		url_tmpl_delete = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/data/%(collection)s/%(id)s?output_mode=json'
		headers = {
			'Authorization': 'Splunk %s' % self.session_key,
			'Content-Type': 'application/json'}

		for key, value in list(delete_event.items()):
			delete_event[key] = value
			if key == '_key' and len(value)>0:
				logger.debug("Found %s (%s) in event" % (key, value))
				try:
					delete_url = url_tmpl_delete % dict(
						server_uri = self.splunkd_uri,
						owner = 'nobody',
						app = self.app,
						collection = self.collection,
						id = urllib.parse.quote(value, safe=''))
					logger.debug("Delete url: " + delete_url)

					try:
						response, response_code = request('DELETE', delete_url, '', headers)
						logger.debug('Server response: %s' % response)
					except BaseException as e:
						logger.error('ERROR Failed to delete key: %s', repr(e))

					if response_code == 200:
						logger.debug("Successfully deleted " + key)
						delete_event['delete_status'] = "success"
						return delete_event
					else:
						logger.error("Error %d deleting %s: %s" % (response_code, key, response))
						delete_event['delete_status'] = "error"
						return delete_event
				except BaseException as e:
					logger.error("Error deleting %s: %s" % (key, repr(e)))
					delete_event['delete_status'] = "error"
					return delete_event

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
		
		self.session_key = self._metadata.searchinfo.session_key
		self.splunkd_uri = self._metadata.searchinfo.splunkd_uri

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
				logger.critical("KVStore collection %s/%s not found" % (self.app, self.collection))
				exit(1)

		except BaseException as e:
			logger.critical('Error enumerating collections: %s' % repr(e))
			exit(1)

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
