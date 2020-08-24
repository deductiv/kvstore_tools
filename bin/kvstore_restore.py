#!/usr/bin/env python

# KV Store App Restore
# Enables restore of backed up KV Store from json or json.gz files

# Author: J.R. Murray <jr.murray@deductiv.net>
# Version: 1.4.1

from future import standard_library
standard_library.install_aliases()
from builtins import str
import sys
from splunk.clilib import cli_common as cli
from splunklib.searchcommands import \
    dispatch, GeneratingCommand, Configuration, Option, validators
import splunklib.client as client
import os
import json
import http.client, urllib.request, urllib.parse, urllib.error
import time
#from datetime import datetime
import gzip
import glob
import re
import logging
import roles

@Configuration()
class KVStoreRestoreCommand(GeneratingCommand):
	""" %(synopsis)

	##Syntax

	| kvstorerestore filename="/data/backup/kvstore/app_name#*#20170130*"

	##Description

	Restores backed up KV Store from json or json.gz files

	"""

	filename = Option(
		doc='''
			Syntax: filename=<filename>
			Description: Specify the file to restore a collection from''',
			require=False)

	append = Option(
		doc='''
			Syntax: append=[true|false]
			Description: Specify whether or not to delete existing entries on the target.''',
			require=False, validate=validators.Boolean())

	#verbose = Option(
	#	doc='''
	#		Syntax: verbose=[true|false]
	#		Description: Specify whether or not to output a row for each batch''',
	#		require=False, validate=validators.Boolean())

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
		errors = []

		# Facility Info in the loglines
		facility = os.path.basename(__file__)
		facility = os.path.splitext(facility)[0]
		facility = facility.replace('_','')
		facility = "[%s]" % (facility)

		# get service object for more infos about this session
		service = client.connect(token=self._metadata.searchinfo.session_key)

		# Check permissions
		required_role = "kv_admin"
		active_user = self._metadata.searchinfo.username
		if active_user in roles.get_role_users(self._metadata.searchinfo.session_key, required_role) or active_user == "admin":
			logger.debug("%s User %s is authorized.", facility, active_user)
		else:
			logger.critical("%s User %s is unauthorized. Has the kv_admin role been granted?", facility, active_user)
			yield({'Error': 'User %s is unauthorized. Has the kv_admin role been granted?' % active_user })
			sys.exit(3)

		logger.info('%s kvstorerestore started', facility)

		try:
			cfg = cli.getConfStanza('kvstore_tools','backups')
			limits_cfg = cli.getConfStanza('limits','kvstore')
		except BaseException as e:
			logger.error("%s Error getting configuration: " + str(e), facility)

		session_key = self._metadata.searchinfo.session_key
		self.logger.debug('%s Session key: %s', facility, session_key)
		splunkd_uri = self._metadata.searchinfo.splunkd_uri

		# Sanitize input
		if self.filename:
			logger.debug('%s Restore filename: %s', facility, self.filename)
			list_only = False
		else:
			self.filename = "*#*#*.json*"
			list_only = True

		if self.append:
			logger.debug('%s Appending to existing collection', facility)
		else:
			self.append = False
			logger.debug('%s Append to existing collection: %s', facility, str(self.append))

		#if self.verbose:
		#	logger.debug('%s Output set to verbose', facility)
		#else:
		#	self.verbose = False

		# Set the headers for HTTP requests
		headers = {
			'Authorization': 'Splunk %s' % session_key,
			'Content-Type': 'application/json'}

		# Set URL templates
		url_tmpl_batch = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/data/%(collection)s/batch_save?output_mode=json'
		url_tmpl = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/data/%(collection)s/?output_mode=json'

		f = []
		if '*' in self.filename:
			# Expand the wildcard to include all matching files from the filesystem
			for name in glob.glob(self.filename):
				f.append(name)

			if len(f) == 0:
				self.filename = os.path.join(cfg.get('default_path'), self.filename)
				for name in glob.glob(self.filename):
					f.append(name)

			if len(f) == 0:
				logger.critical("%s No matching files: " + self.filename, facility)
				sys.exit(1)
		else:
			logger.debug('%s No wildcard string found in %s', facility, self.filename)
			if os.path.isfile(self.filename):
				f.append(self.filename)
			elif os.path.isfile(os.path.join(cfg.get('default_path'), self.filename)):
				f.append(os.path.join(cfg.get('default_path'), self.filename))
			else:
				logger.critical("%s File does not exist: " + self.filename, facility)
				sys.exit(1)

		g = []

		# f is now an array of filenames
		for name in f:
			logger.debug('%s Parsing filename: %s', facility, name)
			try:
				# Isolate the filename from the path
				matches = re.search(r'(.*)(?:\/|\\)([^\/\\]+)', name)
				path = matches.group(1)
				file_param = matches.group(2)
				name_split = file_param.split('#')
			except BaseException as e:
				logger.critical('%s Invalid filename: %s\n\t%s', facility, name, repr(e))
				yield({'Error': 'Invalid filename: ' + name})
				sys.exit(1)

			if name.endswith('.json') and len(name_split)==3:
				fh = open(name, 'r')
			elif name.endswith('.json.gz') and len(name_split)==3:
				logger.debug("%s Compressed file detected.", facility)
				fh = gzip.open(name, 'rb')
				g.append(name)
			elif name.endswith('.tar.gz') or name.endswith('.tgz'):
				logger.info('%s Skipping filename (unsupported format): %s', facility, name)
				yield {'_time': time.time(), 'source': name, 'app': '', 'kvstore': '', 'records': 0, 'result': 'error' }
				continue
			else:
				# Skip this file
				logger.info('%s Skipping filename (does not meet naming convention): %s', facility, name)
				yield {'_time': time.time(), 'source': name, 'app': '', 'kvstore': '', 'records': 0, 'result': 'error' }
				continue
			g.append(name)

			# Extract the app name and collection name from the file name
			file_app = name_split[0]
			file_collection = name_split[1]

			if list_only:
				yield {'filename': name, 'app': file_app, 'collection': file_collection, 'status': 'ready' }

			else:
				try:
					contents = json.loads(fh.read())
				except BaseException as e:
					# Account for a bug in prior versions where the record count could be wrong if "_key" was in the data and the ] would not get appended.
					logger.error("%s Error reading file: %s\n\tAttempting modification (Append ']').", facility, str(e))
					try:
						# Reset the file cursor to 0
						fh.seek(0)
						contents = json.loads(fh.read() + "]")
					except BaseException as e:
						logger.error("%s [Append ']'] Error reading modified json input.\n\tAttempting modification (Strip '[]')", facility)
						try:
							# Reset the file cursor to 0
							fh.seek(0)
							contents = json.loads(fh.read().strip('[]'))
						except BaseException as e:
							logger.error("%s [Strip '[]'] Error reading modified json input for file %s.  Aborting.", facility, name)
							yield {'filename': name, 'app': file_app, 'collection': file_collection, 'status': 'error' }
							continue

				logger.debug("%s File read complete.", facility)
				content_len = len(contents)
				logger.debug('%s File %s entries: %d' % (facility, name, content_len))

				# Build the URL for deleting the collection
				delete_url = url_tmpl % dict(
					server_uri=splunkd_uri,
					owner='nobody',
					app=file_app,
					collection=file_collection)

				# Build the URL for updating the collection
				record_url = url_tmpl_batch % dict(
					server_uri=splunkd_uri,
					owner='nobody',
					app=file_app,
					collection=file_collection)

				if not self.append:
					# Delete the collection contents
					try:
						response = self.request('DELETE', delete_url, "", headers)
						logger.debug('%s Server response for collection deletion: %s', facility, json.dumps(response))
						logger.info("%s Deleted collection: %s\\%s", facility, file_app, file_collection)
					except urllib.error.HTTPError as e:
						logger.error('%s ERROR Failed to delete collection: %s', facility, json.dumps(json.loads(e.read())))
						yield({'Error': 'Failed to delete collection: %s' % json.dumps(json.loads(e.read()))})
						sys.exit(4)

				i = 0
				batch_number = 1
				limit = int(limits_cfg.get('max_documents_per_batch_save'))
				logger.debug("%s Max documents per batch save = %d", facility, limit)
				posted = 0

				while i < content_len:
					# Get the lesser number between (limit-1) and (content_len)
					last = (batch_number*limit)
					last = min(last, content_len)
					batch = contents[i:last]
					i += limit

					logger.debug('%s Batch number: %d (%d bytes / %d records)', facility, batch_number, sys.getsizeof(batch), len(batch))

					# Upload the restored records to the server
					try:
						response = self.request('POST', record_url, str(json.dumps(batch)), headers)
						logger.debug('%s Server response: %d records uploaded.', facility, len(response))
						batch_number += 1
						posted += len(batch)
						#if self.verbose:
						#	yield {'_time': time.time(), 'source': name, 'app': file_app, 'kvstore': file_collection, 'records': posted, 'result': 'success' }
						#	sys.stdout.flush()

					except urllib.error.HTTPError as e:
						logger.error('%s Failed to update records: %s', facility, json.dumps(json.loads(e.read())))
						yield {'_time': time.time(), 'source': name, 'app': file_app, 'kvstore': file_collection, 'records': posted, 'result': 'error' }
						# Force out of the while loop
						i = content_len

				logger.info("%s Restored %d records to %s\\%s", facility, posted, file_app, file_collection)
				# Collection now fully restored
				#if not self.verbose:
				#	yield {'_time': time.time(), 'source': name, 'app': file_app, 'kvstore': file_collection, 'records': posted, 'result': 'complete' }
				#else:
				yield {'_time': time.time(), 'source': name, 'app': file_app, 'kvstore': file_collection, 'records': posted, 'result': 'success' }

dispatch(KVStoreRestoreCommand, sys.argv, sys.stdin, sys.stdout, __name__)
