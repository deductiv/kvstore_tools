#!/usr/bin/env python

# KV Store App Restore
# Enables restore of backed up KV Store from json or json.gz files

# Author: Florian Miehe
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
import gzip
import glob
import re
import logging
import kv_common as kv
from deductiv_helpers import setup_logger, eprint

# Add lib folders to import path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'lib'))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib'))
# pylint: disable=import-error
from splunk.clilib import cli_common as cli
from splunklib.searchcommands import \
    dispatch, GeneratingCommand, Configuration, Option, validators
import splunklib.client as client
import splunk.rest as rest

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
			exit(1)

		logger.info('Script started by %s' % self._metadata.searchinfo.username)

		session_key = self._metadata.searchinfo.session_key
		splunkd_uri = self._metadata.searchinfo.splunkd_uri

		# Check for permissions to run the command
		content = rest.simpleRequest('/services/authentication/current-context?output_mode=json', sessionKey=session_key, method='GET')[1]
		content = json.loads(content)
		current_user = self._metadata.searchinfo.username
		current_user_capabilities = content['entry'][0]['content']['capabilities']
		if 'run_kvstore_restore' in current_user_capabilities or 'run_kvst_all' in current_user_capabilities:
			logger.debug("User %s is authorized." % current_user)
		else:
			logger.error("User %s is unauthorized. Has the run_kvstore_restore capability been granted?" % current_user)
			yield({'Error': 'User %s is unauthorized. Has the run_kvstore_restore capability been granted?' % current_user })
			sys.exit(3)

		# Sanitize input
		if self.filename:
			logger.debug('Restore filename: %s' % self.filename)
			list_only = False
		else:
			self.filename = "*#*#*.json*"
			list_only = True

		if self.append:
			logger.debug('Appending to existing collection')
		else:
			self.append = False
			logger.debug('Append to existing collection: %s' % str(self.append))

		backup_file_list = []

		# Get the default path from the configuration
		default_path_dirlist = cfg.get('default_path').split('/')
		default_path = os.path.abspath(os.path.join(os.sep, *default_path_dirlist))
		# Replace environment variables
		default_path = os.path.expandvars(default_path)
		default_path = default_path.replace('//', '/')

		if '*' in self.filename:
			# Expand the wildcard to include all matching files from the filesystem
			for name in glob.glob(self.filename):
				backup_file_list.append(name)

			if len(backup_file_list) == 0:
				self.filename = os.path.join(default_path, self.filename)
				for name in glob.glob(self.filename):
					backup_file_list.append(name)

			if len(backup_file_list) == 0:
				logger.critical("No matching files: %s" % self.filename)
				sys.exit(1)
		else:
			logger.debug('No wildcard string found in %s' % self.filename)
			if os.path.isfile(self.filename):
				backup_file_list.append(self.filename)
			elif os.path.isfile(os.path.join(default_path, self.filename)):
				backup_file_list.append(os.path.join(default_path, self.filename))
			else:
				logger.critical("File does not exist: %s" % self.filename)
				sys.exit(1)

		deleted_collections = []

		# f is now an array of filenames
		for name in backup_file_list:
			logger.debug('Parsing filename: %s' % name)
			try:
				# Isolate the filename from the path
				matches = re.search(r'(.*)(?:\/|\\)([^\/\\]+)', name)
				#path = matches.group(1)
				file_param = matches.group(2)
				name_split = file_param.split('#')
			except BaseException as e:
				logger.critical('Invalid filename: %s\n\t%s' % (name, repr(e)))
				yield({'Error': 'Invalid filename: %s' % name})
				sys.exit(1)

			# Open the file if it's a supported format
			if (name.endswith('.json') or name.endswith('.json.gz')) and len(name_split)==3:

				# Extract the app name and collection name from the file name
				file_app = name_split[0]
				file_collection = name_split[1]

				if list_only:
					yield {'filename': name, 'app': file_app, 'collection': file_collection, 'status': 'ready' }
				else:
					if not self.append:
						# Delete the collection contents using the KV Store REST API
						try:
							collection_id = file_app + "/" + file_collection
							# Make sure we aren't trying to delete the same collection twice
							if not collection_id in deleted_collections:
								kv.delete_collection(logger, splunkd_uri, session_key, file_app, file_collection)
								deleted_collections.append(collection_id)
						except BaseException as e:
							logger.critical(repr(e), exc_info=True)
							yield({'Error': 'Failed to delete collection %s/%s: %s' % (file_app, file_collection, repr(e))})
							sys.exit(4)
					
					# Upload the collection to the KV Store REST API
					try:
						result, message, record_count = kv.upload_collection(logger, splunkd_uri, session_key, file_app, file_collection, name)
						yield({ 'result': result, 'message': message, 'records': record_count })
					except BaseException as e:
						logger.error("Error restoring collection: %s" % repr(e), exc_info=True)
						yield({'result': 'error', 'message': 'Failed to delete collection: %s' % repr(e), 'records': 0})

			elif name.endswith('.tar.gz') or name.endswith('.tgz'):
				logger.info('Skipping filename (unsupported format): %s' % name)
				yield {'_time': time.time(), 'source': name, 'app': '', 'collection': '', 'records': 0, 'result': 'error' }
				continue
			else:
				# Skip this file
				logger.info('Skipping filename (does not meet naming convention): %s' % name)
				yield {'_time': time.time(), 'source': name, 'app': '', 'collection': '', 'records': 0, 'result': 'error' }
				continue

dispatch(KVStoreRestoreCommand, sys.argv, sys.stdin, sys.stdout, __name__)
