#!/usr/bin/env python

# KV Store Collection Record Deleter
# Deletes a specific record from a KV Store collection based on _key value

# Author: J.R. Murray <jr.murray@deductiv.net>
# Version: 2.0.0

from future import standard_library
standard_library.install_aliases()
from builtins import str
import sys
import os
import json
import urllib.parse
import time
#from datetime import datetime
import gzip
import glob
import re
import logging
import kv_common as kv
from deductiv_helpers import setup_logger, request, eprint

# Add lib folders to import path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'lib'))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib'))
# pylint: disable=import-error
from splunk.clilib import cli_common as cli
from splunklib.searchcommands import \
	dispatch, GeneratingCommand, Configuration, Option, validators
from splunklib.client import connect
import splunk.rest as rest

@Configuration()
class KVStoreDeleteKeyCommand(GeneratingCommand):
	""" %(synopsis)

	##Syntax

	| deletekey app="app_name" collection="collection_name" key="key_id"

	##Description

	Deletes a specific record from a collection based on _key value

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

	key = Option(
		doc='''
		 Syntax: key=<key_value>
		 Description: Specify the record to delete within the collection''',
		 require=True)

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
		
		session_key = self._metadata.searchinfo.session_key
		splunkd_uri = self._metadata.searchinfo.splunkd_uri

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
		
		if self.key:
			logger.debug('Key ID: %s' % self.collection)
		else:
			logger.critical("No key value specified. Exiting.")
			print("Error: No key value specified.")
			exit(1)

		headers = {
			'Authorization': 'Splunk %s' % session_key,
			'Content-Type': 'application/json'}
		#url_tmpl_app = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/config?output_mode=json&count=0'

		# Enumerate all apps
		app_list = kv.get_server_apps(splunkd_uri, session_key, self.app)
		collection_list = kv.get_app_collections(splunkd_uri, session_key, self.collection, self.app, app_list, True)
		
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
					logger.debug("Collection found: {0} in app {1}".format(self.collection, self.app))
			if not collection_present:
				logger.critical("KVStore collection %s not found within app %s" % (self.collection, self.app))
				exit(1)

		except BaseException as e:
			logger.critical('Error enumerating collections: ' + str(e))
			exit(1)

		url_tmpl_delete = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/data/%(collection)s/%(id)s?output_mode=json'
		try:
			delete_url = url_tmpl_delete % dict(
				server_uri = splunkd_uri,
				owner = 'nobody',
				app = self.app,
				collection = self.collection,
				id = urllib.parse.quote(self.key, safe=''))
			logger.debug("Delete url: " + delete_url)

			try:
				response, response_code = request('DELETE', delete_url, '', headers)
				logger.debug('Server response: %s', response)
			except BaseException as e:
				logger.error('Failed to delete key %s from collection %s/%s: %s' % (self.key, self.app, self.collection, repr(e)))

			if response_code == 200:
				logger.debug("Successfully deleted key %s from collection %s/%s" % (self.key, self.app, self.collection))
				result = "success"
			else:
				logger.error("Error deleting key %s from collection %s/%s: %s" % (self.key, self.app, self.collection, response))
				result = "error"

		except BaseException as e:
			logger.error("Error deleting key %s from collection %s/%s: %s" % (self.key, self.app, self.collection, repr(e)))
			result = "error"

		# Entry deleted
		yield {'_time': time.time(), 'app': self.app, 'collection': self.collection, 'key': self.key, 'result': result }

dispatch(KVStoreDeleteKeyCommand, sys.argv, sys.stdin, sys.stdout, __name__)
