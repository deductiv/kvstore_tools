#!/usr/bin/env python

# KV Store Collection Record Deleter
# Deletes a specific record from a KV Store collection based on _key value

# Author: J.R. Murray <jr.murray@deductiv.net>
# Version: 1.4.1

from future import standard_library
standard_library.install_aliases()
from builtins import str
import sys
from splunk.clilib import cli_common as cli
from splunklib.searchcommands import \
	dispatch, GeneratingCommand, Configuration, Option, validators
from splunklib.client import connect
import splunk.rest as rest
import os
import json
import http.client, urllib.request, urllib.parse, urllib.error
import urllib.request, urllib.error, urllib.parse
import time
#from datetime import datetime
import gzip
import glob
import re
import logging
import roles

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

	splunkd_uri = None
	session_key = None

	def request(self, method, url, data=None, headers=None):
		"""Helper function to fetch JSON data from the given URL"""
		#logger = logging.getLogger('kvst')
		if data is not None:
			try:
				data = urllib.parse.urlencode(data).encode("utf-8")
			except:
				data = data.encode("utf-8")

		#req = urllib.request.Request(url, data, headers)
		if headers is None:
			req = urllib.request.Request(url, data=data, method=method)
		else:
			req = urllib.request.Request(url, data=data, headers=headers, method=method)

		with urllib.request.urlopen(req) as res:
			res_txt = res.read().decode('utf-8')
			#logger.debug(res_txt)
			res_code = res.getcode()
			#logger.debug(res_code)
			if len(res_txt)>0:
				return json.loads(res_txt)
			else:
				return res_code

	def generate(self):
		logger = logging.getLogger('kvst')
		logger.info('deletekey started')

		# Sanitize input
		if len(self.key) > 0:
			self.logger.debug('Delete _key %s from %s', self.key, self.collection)
		else:
			exit(1)

		self.session_key = self._metadata.searchinfo.session_key
		self.splunkd_uri = self._metadata.searchinfo.splunkd_uri

		# Check permissions
		#required_role = "kv_admin"
		#active_user = self._metadata.searchinfo.username
		#if active_user in roles.get_role_users(self._metadata.searchinfo.session_key, required_role) or active_user == "admin":
		#	logger.debug("%s User %s is authorized.", facility, active_user)
		#else:
		#	logger.error("%s User %s is unauthorized. Has the kv_admin role been granted?", facility, active_user)
		#	exit(3)

		headers = {
			'Authorization': 'Splunk %s' % self.session_key,
			'Content-Type': 'application/json'}
		url_tmpl_app = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/config?output_mode=json&count=0'

		apps = []

		# Enumerate all apps
		try:
			response, content = rest.simpleRequest("apps/local?output_mode=json&count=0", sessionKey=self.session_key, method='GET')

			logger.debug('Server response: %s', response)
			content = json.loads(content)
			for entry in content["entry"]:
				if not entry["content"]["disabled"]:
					apps.append(entry["name"])

		except urllib.error.HTTPError as e:
			logger.critical('ERROR Failed to create app list: %s', json.dumps(json.loads(e.read())))
			sys.exit(3)

		collections = []
		for app in apps:
			logger.debug("Polling collections in app: %s" % app)
			# Enumerate all of the collections in the app
			collections_url = url_tmpl_app % dict(
				server_uri = self.splunkd_uri,
				owner = 'nobody',
				app = app)

			try:
				response = self.request('GET', collections_url, '', headers)
				#logger.debug('Server response: %s', json.dumps(response))
			except urllib.error.HTTPError as e:
				logger.critical('ERROR Failed to download collection list: %s', json.dumps(json.loads(e.read())))
				sys.exit(3)

			logger.debug("Parsing response for collections in app: %s" % app)
			for entry in response["entry"]:
				entry_app = entry["acl"]["app"]
				collection_name = entry["name"]
				c = [entry_app, collection_name]
				if c not in collections:
					collections.append(c)

		logger.debug('Collections present: %s', str(collections))

		try:
			# Create an object for the collection
			collection_present = False
			for c in collections:
			# Extract the app and collection name from the array
			# c[0] = app, c[1] = collection name
				if (c[1] == self.collection):
					if self.app is None or self.app == c[0]:
						self.app = c[0]
						collection_present = True
					elif self.app != c[0]:
						pass
					logger.debug("Collection found: {0} in app {1}".format(self.collection, self.app))
			if not collection_present:
				logger.critical("KVStore collection not found: %s" % self.collection)
				exit(1)

		except BaseException as e:
			logger.critical('Error enumerating collections: ' + str(e))
			exit(1)

		url_tmpl_delete = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/data/%(collection)s/%(id)s?output_mode=json'
		try:
			delete_url = url_tmpl_delete % dict(
				server_uri = self.splunkd_uri,
				owner = 'nobody',
				app = self.app,
				collection = self.collection,
				id = urllib.parse.quote(self.key, safe=''))
			logger.debug("Delete url: " + delete_url)

			try:
				response = self.request('DELETE', delete_url, '', headers)
				logger.debug('Server response: %s', json.dumps(response))
			except urllib.error.HTTPError as e:
				logger.error('ERROR Failed to delete key: %s', json.dumps(json.loads(e.read())))

			if response == 200:
				logger.debug("Successfully deleted " + self.key)
				result = "success"
			else:
				logger.error("Error deleting {0}: {1}".format(self.key, json.dumps(response)))
				result = "error"

		except BaseException as exc:
			logger.error("Error deleting {0}: {1}".format(self.key, str(exc)))
			result = "error"

		# Entry deleted
		yield {'_time': time.time(), 'app': self.app, 'collection': self.collection, 'key': self.key, 'result': result }

dispatch(KVStoreDeleteKeyCommand, sys.argv, sys.stdin, sys.stdout, __name__)
