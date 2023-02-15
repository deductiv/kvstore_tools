#!/usr/bin/env python

# KV Store Backup
# Enables backup of collections on a per-app basis
# Enumerates collections for an app and backs up JSON for each one

# Author: J.R. Murray <jr.murray@deductiv.net>
# Version: 2.0.9

from __future__ import division
from __future__ import print_function
from future import standard_library
standard_library.install_aliases()
from builtins import str
from past.utils import old_div
import sys, os
import stat
import json
import time
from datetime import datetime
import glob
import kv_common as kv
from deductiv_helpers import setup_logger, eprint, search_console
from splunk.clilib import cli_common as cli
import splunk.rest as rest

# Add lib folders to import path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib'))
from splunklib.searchcommands import \
    dispatch, GeneratingCommand, Configuration, Option, validators

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
			Description: Specify the app to backup collections from
			Default: All ''',
			require=False)

	path = Option(
		doc='''
			Syntax: path=<filename>
			Description: Specify the path to backup collections to
			Default: Specified in app configuration ''',
			require=False)

	global_scope = Option(
		doc='''
			Syntax: global_scope=[true|false]
			Description: Specify the whether or not to include all globally available collections
			Default: False ''',
			require=False, validate=validators.Boolean())

	collection = Option(
		doc='''
			Syntax: collection=<collection_name>
			Description: Specify the collection to backup within the specified app
			Default: All ''',
			require=False)

	compression = Option(
		doc='''
			Syntax: compression=[true|false]
			Description: Specify whether or not to compress the backups
			Default: Specified in app configuration ''',
			require=False, validate=validators.Boolean())

	def generate(self):
		try:
			cfg = cli.getConfStanza('kvstore_tools','settings')
		except BaseException as e:
			self.write_error("Could not read configuration: " + repr(e))
			exit(1)
		
		# Facility info - prepended to log lines
		facility = os.path.basename(__file__)
		facility = os.path.splitext(facility)[0]
		logger = setup_logger(cfg["log_level"], 'kvstore_tools.log', facility)
		ui = search_console(logger, self)
		logger.info('Script started by %s' % self._metadata.searchinfo.username)

		batch_size = int(cfg.get('backup_batch_size'))
		logger.debug("Batch size: %d rows" % batch_size)
		session_key = self._metadata.searchinfo.session_key
		splunkd_uri = self._metadata.searchinfo.splunkd_uri

		# Check for permissions to run the command
		content = rest.simpleRequest('/services/authentication/current-context?output_mode=json', sessionKey=session_key, method='GET')[1]
		content = json.loads(content)
		current_user = self._metadata.searchinfo.username
		current_user_capabilities = content['entry'][0]['content']['capabilities']
		if 'run_kvstore_backup' in current_user_capabilities or 'run_kvst_all' in current_user_capabilities or current_user == 'splunk-system-user':
			logger.debug("User %s is authorized." % current_user)
		else:
			ui.exit_error("User %s is unauthorized. Has the run_kvstore_backup capability been granted?" % current_user)

		# Sanitize input
		if self.app:
			logger.debug('App Context: %s' % self.app)
		else:
			self.app = None

		if self.path:
			pass
		else:
			# Get path from configuration
			try:
				# Break path out and re-join it so it's OS independent
				default_path = cfg.get('default_path').split('/')
				self.path = os.path.abspath(os.path.join(os.sep, *default_path))
			except:
				ui.exit_error("Unable to get backup path. Path not provided in search arguments and default path is not set.")
		
		# Replace environment variables
		self.path = os.path.expandvars(self.path)
		self.path = self.path.replace('//', '/')
		logger.debug('Backup path: %s' % self.path)
		if not os.path.isdir(self.path):
			ui.exit_error("Path does not exist: {0}".format(self.path))

		if self.collection:
			logger.debug('Collection: %s' % self.collection)
		else:
			self.collection = None

		if self.global_scope:
			logger.debug('Global Scope: %s' % self.global_scope)
		else:
			self.global_scope = False

		if self.compression or self.compression == False:
			logger.debug('Compression: %s' % self.compression)
		else:
			try:
				self.compression = cfg.get('compression')
			except:
				self.compression = False

		app_list = kv.get_server_apps(splunkd_uri, session_key, self.app)
		logger.debug("Apps list: %s" % str(app_list))
		collection_list = kv.get_app_collections(splunkd_uri, session_key, self.collection, self.app, app_list, self.global_scope)

		logger.info('Collections to backup: %s', str(collection_list))

		for collection in collection_list:
			# Extract the app and collection name from the array
			entry_app = collection[0]
			collection_name = collection[1]

			ts = time.time()
			st = datetime.fromtimestamp(ts).strftime('%Y%m%d_%H%M%S')
			#maxrows = int(limits_cfg.get('max_rows_per_query'))

			# Set the filename and location for the output (expanding environment variables)
			output_filename = entry_app + "#" + collection_name + "#" + st + ".json"
			if self.compression:
				output_filename = output_filename + '.gz'
			output_file = os.path.join(self.path, output_filename)

			# Download the collection to a local file
			result, message, total_record_count = kv.download_collection(logger, splunkd_uri, session_key, entry_app, collection_name, output_file, self.compression)
			logger.debug("Retrieved {0} records from {1}".format(total_record_count, collection_name))
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
			backup_file_list = glob.glob(pattern)

			# Sort descending based on file timestamp
			backup_file_list.sort(key=os.path.getmtime, reverse=True)

			# Count the total bytes in all of the files
			totalbytes = 0
			logger.debug("Max age (days): %s / Max size: %s" % (max_age, max_size))
			
			for f in backup_file_list:
				logger.debug("File %s", f)

				# Get the file size (bytes) and age (days)
				bytes = os.path.getsize(f)
				age_days = old_div((time.time() - os.stat(f)[stat.ST_MTIME]), 86400)
				logger.debug("Age (days): %d", age_days)

				# increment the total byte count
				totalbytes += bytes

				if totalbytes > max_size and max_size > 0:
					# Delete the files
					logger.debug("Total bytes ({0}) > max_size ({1})".format(totalbytes, max_size))
					os.remove(f)
					logger.info("Deleted file due to size retention policy: %s" % f)

				elif age_days > max_age and max_age > 0:
					logger.debug("Age ({0}) > max_age ({1})".format(age_days, max_age))
					os.remove(f)
					logger.info("Deleted file due to age retention policy: %s" % f)

dispatch(KVStoreBackupCommand, sys.argv, sys.stdin, sys.stdout, __name__)
