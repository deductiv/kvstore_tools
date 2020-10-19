#!/usr/bin/env python

# KV Store Backup
# Enables backup of collections on a per-app basis
# Enumerates collections for an app and backs up JSON for each one

# Author: J.R. Murray <jr.murray@deductiv.net>
# Version: 1.4.1

from __future__ import division
from __future__ import print_function
from future import standard_library
standard_library.install_aliases()
from builtins import str
from past.utils import old_div
import sys, os

# Add lib folders to import path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'lib'))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib'))

from splunk.clilib import cli_common as cli
from splunklib.searchcommands import \
    dispatch, GeneratingCommand, Configuration, Option, validators
from splunk.clilib import cli_common as cli
from splunklib.client import connect
import splunk.rest as rest
import stat
import json
import http.client, urllib.request, urllib.parse, urllib.error
import urllib.request, urllib.error, urllib.parse
import time
from datetime import datetime
import gzip
import glob
import shutil
import logging
import roles

@Configuration()
class KVStoreBackupCommand(GeneratingCommand):
	""" %(synopsis)

	##Syntax

	| kvstorebackup app="app_name" collection="collection_name" path="/data/backup/kvstore" global_scope="false"

	##Description

	Backs up each collection in the KV Store to a JSON file in the path specified

	"""

	app = Option(
		doc='''
			Syntax: app=<appname>
			Description: Specify the app to backup collections from''',
			require=False)

	path = Option(
		doc='''
			Syntax: path=<filename>
			Description: Specify the path to backup collections to''',
			require=False)

	global_scope = Option(
		doc='''
			Syntax: global_scope=[true|false]
			Description: Specify the whether or not to include all globally available collections''',
			require=False, validate=validators.Boolean())

	collection = Option(
		doc='''
			Syntax: collection=<collection_name>
			Description: Specify the collection to backup within the specified app''',
			require=False)

	compression = Option(
		doc='''
			Syntax: compression=[true|false]
			Description: Specify whether or not to compress the backups''',
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

		# Facility Info in the loglines
		facility = os.path.basename(__file__)
		facility = os.path.splitext(facility)[0]
		facility = facility.replace('_','')
		facility = "[%s]" % (facility)

		logger.debug('%s KVStoreBackupCommand: %s', facility, self)
		errors = []

		# get service object for more infos about this session
		service = connect(token=self._metadata.searchinfo.session_key)

		# Check permissions
		required_role = "kv_admin"
		active_user = self._metadata.searchinfo.username
		if active_user in roles.get_role_users(self._metadata.searchinfo.session_key, required_role) or active_user == "admin":
			logger.debug("%s User %s is authorized.", facility, active_user)
		else:
			logger.error("%s User %s is unauthorized. Has the kv_admin role been granted?", facility, active_user)
			yield({'Error': 'User {0} is unauthorized. Has the kv_admin role been granted?'.format(active_user) })
			sys.exit(3)

		logger.info('%s kvstorebackup started', facility)

		try:
			cfg = cli.getConfStanza('kvstore_tools','backups')
			limits_cfg = cli.getConfStanza('limits','kvstore')
		except BaseException as e:
			logger.error("%s Error getting configuration: " + str(e), facility)

		batch_size = int(cfg.get('backup_batch_size'))
		logger.debug("%s Batch size: %d rows", facility, batch_size)
		session_key = self._metadata.searchinfo.session_key
		splunkd_uri = self._metadata.searchinfo.splunkd_uri

		# Sanitize input
		if self.app:
			logger.debug('%s App Context: %s', facility, self.app)
		else:
			self.app = None

		if self.path:
			pass
		else:
			# Get path from configuration
			try:
				self.path = cfg.get('default_path')
			except:
				logger.critical("%s Unable to get backup path", facility)
				yield({'Error': "Path not provided in search arguments and default path is not set."})
				sys.exit(1)

		self.path = os.path.expandvars(self.path)
		logger.debug('%s Backup path: %s', facility, self.path)
		if not os.path.isdir(self.path):
			logger.critical("%s Path does not exist: {0}".format(self.path), facility)
			yield({'Error': "Path does not exist: {0}".format(self.path)})
			sys.exit(1)

		if self.collection:
			logger.debug('%s Collection: %s', facility, self.collection)
		else:
			self.collection=None

		if self.global_scope:
			logger.debug('%s Global Scope: %s', facility, self.global_scope)
		else:
			self.global_scope = False

		if self.compression:
			logger.debug('%s Compression: %s', facility, self.compression)
		else:
			try:
				self.compression = cfg.get('compression')
			except:
				self.compression = False

		headers = {
			'Authorization': 'Splunk %s' % session_key,
			'Content-Type': 'application/json'}
		url_tmpl_app = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/config?output_mode=json&count=0'

		apps = []

		if self.app is not None:
			apps.append(self.app)
		else:
			# Enumerate all apps
			try:
				response, content = rest.simpleRequest("apps/local?output_mode=json", sessionKey=session_key, method='GET')

				logger.debug('%s Server response: %s', facility, response)
				#logger.debug('%s Server content: %s', facility, content)
				content = json.loads(content)
				for entry in content["entry"]:
					if not entry["content"]["disabled"]:
						apps.append(entry["name"])

			except urllib.error.HTTPError as e:
				logger.critical('%s ERROR Failed to create app list: %s', facility, json.dumps(json.loads(e.read())))
				yield({'Error': 'Failed to create app list: {0}}'.format(json.dumps(json.loads(e.read())))})
				sys.exit(3)

		collections = []
		for app in apps:
			logger.debug("%s Polling collections in app: %s", facility, app)
			# Enumerate all of the collections in the app (if an app is selected)
			collections_url = url_tmpl_app % dict(
				server_uri=splunkd_uri,
				owner='nobody',
				app=app)

			try:
				response = self.request('GET', collections_url, '', headers)
				#logger.debug('%s Server response: %s', facility, json.dumps(response))
			except urllib.error.HTTPError as e:
				logger.critical('%s ERROR Failed to download collection list: %s', facility, json.dumps(json.loads(e.read())))
				yield({'Error': 'Failed to download collection list: {0}'.format(json.dumps(json.loads(e.read())))})
				sys.exit(4)

			logger.debug("%s Parsing response for collections in app: %s", facility, app)
			for entry in response["entry"]:
				entry_app = entry["acl"]["app"]
				collection_name = entry["name"]
				#logger.debug(entry_app)
				#logger.debug(collection_name)
				sharing = entry["acl"]["sharing"]

				#logger.debug("%s Parsing entry: %s" % str(entry))

				if (self.app == entry_app and self.collection == collection_name) or (self.app is None and self.collection == collection_name) or (self.app == entry_app and self.collection is None) or (sharing == 'global' and self.global_scope) or (self.app is None and self.collection is None):

					#if entry_app == self.app or self.global_scope:
					#if self.collection == '' or self.collection == collection_name:
					c = [entry_app, collection_name]
					if c not in collections:
						collections.append(c)
					logger.debug("%s Added {0}/{1} to backup list".format(entry_app, collection_name), facility)

		logger.info('%s Collections to backup: %s', facility, str(collections))

		url_tmpl_collection = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/data/%(collection)s?limit=%(limit)s&skip=%(skip)s&output_mode=json'

		for collection in collections:
			# Extract the app and collection name from the array
			entry_app = collection[0]
			collection_name = collection[1]


			loop_record_count = None
			total_record_count = 0
			message = None
			ts = time.time()
			st = datetime.fromtimestamp(ts).strftime('%Y%m%d_%H%M%S')
			maxrows = int(limits_cfg.get('max_rows_per_query'))

			# Set the filename and location for the output (expanding environment variables)
			output_filename = entry_app + "#" + collection_name + "#" + st + ".json"
			output_file = os.path.join(self.path, output_filename)

			try:
				cursor = 0
				with open(output_file, "w") as f:

					# If the loop record count is equal to batch size, we hit the limit. Keep going.
					while (loop_record_count is None or loop_record_count == batch_size):
						# Build the URL
						data_url = url_tmpl_collection % dict(
							server_uri=splunkd_uri,
							owner='nobody',
							app=entry_app,
							collection=collection_name,
							limit = batch_size,
							skip = cursor)

						# Download the data from the collection
						response = self.request('GET', data_url, '', headers)
						# Remove the first and last characters ( [ and ] )
						response = json.dumps(response)[1:-1]
						# Insert line breaks in between records -- "}, {"
						response = response.replace('}, {', '}, \n{')
						#logger.debug("%s Response: " + str(response))
						loop_record_count = response.count('"_key"')
						total_record_count += loop_record_count
						# Append the records to the file
						if loop_record_count > 0:
							# Write the leading [ or comma delimiter (between batches)
							if cursor == 0:
								f.write('[')
							else:
								f.write(',')
							f.write(response)
							if loop_record_count < batch_size:
								f.write(']')
						cursor += loop_record_count

				logger.debug("%s Retrieved {0} records from {1}".format(total_record_count, collection_name), facility)

				if total_record_count > 0:
					if self.compression:
						try:
							with open(output_file, 'rb') as f_in, gzip.open(output_file + '.gz', 'wb') as f_out:
								shutil.copyfileobj(f_in, f_out)
							os.remove(output_file)
							output_file = output_file + '.gz'
						except BaseException as e:
							logger.warn('%s Error compressing file ' + output_file + '\n\t' + repr(e), facility)

					if total_record_count == maxrows:
						logger.warning('%s Backed up KV store collection up to the limit: %s/%s', facility, entry_app, collection_name)
						result = "warning"
						message = "Rows returned equal to configured limit. Possible incomplete backup."
					if batch_size > maxrows and total_record_count > maxrows:
						logger.warning('%s Backed up KV store collection with batches exceeding the limit: %s/%s', facility, entry_app, collection_name)
						result = "warning"
						message = "Batch size greater than configured query limit. Possible incomplete backup."
					else:
						logger.info('%s Backed up KV store collection successfully: %s/%s', facility, entry_app, collection_name)
						result = "success"
				else:
					result = "skipped"
					message = "Collection is empty."
					if os.path.isfile(output_file):
						os.remove(output_file)
					output_file = None

			except BaseException as e:
				logger.debug('%s ERROR Failed to download collection: %s', facility, str(e))
				result = "error"
				record_count = 0
				errors.append('Error downloading collection: ' + entry_app + '/' + collection_name)

			yield {'_time': time.time(), 'app': entry_app, 'collection': collection_name, 'result': result, 'records': total_record_count, 'message': message, 'file': output_file }

		# Execute retention routine
		max_age = 0
		max_age = int(cfg.get('retention_days'))
		max_size = 0
		max_size = int(cfg.get('retention_size')) * 1024 * 1024

		if max_size > 0 or max_age > 0:
			# Check the size of all *.json and *.json.gz files in the directory
			#dir = self.path
			pattern = os.path.join(self.path, "*#*#*.json*")

			# Get a listing of the files and check the file sizes
			l = glob.glob(pattern)

			# Sort descending based on file timestamp
			l.sort(key=os.path.getmtime, reverse=True)

			# Count the total bytes in all of the files
			totalbytes = 0
			logger.debug("%s Max age (days): %s / Max size: %s", facility, max_age, max_size)
			
			for f in l:
				logger.debug("%s File %s", facility, f)

				# Get the file size (bytes) and age (days)
				bytes = os.path.getsize(f)
				age_days = old_div((time.time() - os.stat(f)[stat.ST_MTIME]), 86400)
				logger.debug("%s Age (days): %d", facility, age_days)

				# increment the total byte count
				totalbytes += bytes

				if totalbytes > max_size and max_size > 0:
					# Delete the files
					logger.debug("%s totalbytes ({0}) > max_size ({1})".format(totalbytes, max_size), facility)
					os.remove(f)
					logger.info("%s Deleted file due to size retention policy: %s", facility, f)

				elif age_days > max_age and max_age > 0:
					logger.debug("%s age ({0}) > max_age ({1})".format(age_days, max_age), facility)
					os.remove(f)
					logger.info("%s Deleted file due to age retention policy: %s", facility, f)

		if len(errors) > 0:
			message = "Errors generated during backup \n" + '\n'.join(errors)
		else:
			message = "Success"
		logger.debug(message)

dispatch(KVStoreBackupCommand, sys.argv, sys.stdin, sys.stdout, __name__)
