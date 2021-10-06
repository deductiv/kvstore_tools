# kv_common.py
# Functions for managing KV Store collection data

# Author: J.R. Murray <jr.murray@deductiv.net>
# Version: 2.0.4

from builtins import str
import os, sys
import json
import time
from datetime import datetime, timedelta
import gzip
import re
from deductiv_helpers import eprint, request

# Add lib folders to import path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'lib'))
# pylint: disable=import-error
import splunk.rest as rest 
from splunk.clilib import cli_common as cli 
from splunksecrets import decrypt

def get_server_apps(uri, session_key, app = None):
	apps = []
	if app is not None:
		apps.append(app)
	else:
		# Enumerate all remote apps
		apps_uri = uri + '/services/apps/local?output_mode=json'
		content = rest.simpleRequest(apps_uri, sessionKey=session_key, method='GET')[1]
		content = json.loads(content)
		for entry in content["entry"]:
			if not entry["content"]["disabled"]:
				apps.append(entry["name"])
	return apps

def get_app_collections(uri, session_key, selected_collection, selected_app, app_list, global_scope):
	url_tmpl_app = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/config?output_mode=json&count=0'

	# Enumerate all collections in the apps list
	collections = []
	for app in app_list:
		eprint("Polling collections in app: %s" % app)
		# Enumerate all of the collections in the app (if an app is selected)
		collections_url = url_tmpl_app % dict(
			server_uri = uri,
			owner = 'nobody',
			app = app)
		headers = {
			'Authorization': 'Splunk %s' % session_key,
			'Content-Type': 'application/json'}
		
		try:
			response, response_code = request('GET', collections_url, '', headers)
			if response_code == 200:
				response = json.loads(response)
			else:
				# There's a problem connecting. Abort.
				raise Exception("Could not connect to server: Error %s" % response_code)
		except BaseException as e:
			raise Exception(e)

		for entry in response["entry"]:
			entry_app = entry["acl"]["app"]
			entry_collection = entry["name"]
			entry_sharing = entry["acl"]["sharing"]
			eprint(entry_sharing + '/' + entry_app + '/' + entry_collection)

			if ((selected_app == entry_app and selected_collection == entry_collection) or 
				(selected_app is None and selected_collection == entry_collection) or
				(selected_app == entry_app and selected_collection is None) or
				(entry_sharing == 'global' and global_scope) or 
				(selected_app is None and selected_collection is None)):
				
				c = [entry_app, entry_collection]
				if c not in collections:
					collections.append(c)
					eprint("Added {0}/{1} to backup list".format(entry_app, entry_collection))
	return collections

def copy_collection(logger, source_session_key, source_uri, target_session_key, target_uri, app, collection, append):
	# Enumerate all of the collections in the app (if an app is selected)
	#collection_contents = download_collection(logger, source_uri, app, collection)
	source_host = hostname_from_uri(source_uri)
	target_host = hostname_from_uri(target_uri)

	# Download the collection
	cfg = cli.getConfStanza('kvstore_tools','settings')
	staging_dir = os.path.expandvars(os.path.join(cfg["default_path"], 'staging'))

	ts = time.time()
	st = datetime.fromtimestamp(ts).strftime('%Y%m%d_%H%M%S')

	# Set the filename and location for the output (expanding environment variables)
	output_filename = app + "#" + collection + "#" + st + ".json.gz"
	output_file = os.path.join(staging_dir, output_filename)
	output_file = os.path.expandvars(output_file)

	# Create the directory recursively if it does not exist
	os.makedirs(staging_dir, exist_ok=True)

	# Download the collection to a file (compressed)
	try:
		download_start_time = time.time()
		result, message, record_count = download_collection(logger, source_uri, source_session_key, app, collection, output_file, True)
		download_end_time = time.time()
		if result == "success":
			if not append:
				# Delete the target collection prior to uploading
				delete_start_time = time.time()
				response_code = delete_collection(logger, target_uri, target_session_key, app, collection)
				delete_end_time = time.time()
				logger.debug("Response code for copy collection deletion request: %d" % response_code)

			upload_start_time = time.time()
			result, message, posted = upload_collection(logger, target_uri, target_session_key, app, collection, output_file)
			upload_end_time = time.time()

			download_time = str(timedelta(seconds=(download_end_time - download_start_time)))
			try:
				delete_time = str(timedelta(seconds=(delete_end_time - delete_start_time)))
			except:
				delete_time = None
			
			upload_time = str(timedelta(seconds=(upload_end_time - upload_start_time)))

			if result == "success": 
				# Delete the output file
				if os.path.exists(output_file):
					os.remove(output_file)
					return { "app": app, "collection": collection, "result": result, 
						"download_time": download_time, "delete_time": delete_time, 
						"upload_time": upload_time, "download_count": record_count, "upload_count": posted}
			else:
				result = "error"
				return { "app": app, "collection": collection, "result": result, 
					"download_time": None, "delete_time": None, 
					"upload_time": None, "download_count": record_count, "upload_count": posted }
		else:
			raise Exception("Download not successful")
	except BaseException as e:
		raise Exception("Error copying the collection from %s to %s: %s" % (source_host, target_host, repr(e)))

def delete_collection(logger, remote_uri, remote_session_key, app, collection):
	# Build the URL for deleting the collection
	url_tmpl = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/data/%(collection)s/?output_mode=json'
	delete_url = url_tmpl % dict(
		server_uri = remote_uri,
		owner = 'nobody',
		app = app,
		collection = collection)
	
	# Set request headers
	headers = {
		'Authorization': 'Splunk %s' % remote_session_key,
		'Content-Type': 'application/json'}
	
	hostname = hostname_from_uri(remote_uri)

	# Delete the collection contents
	try:
		response, response_code = request('DELETE', delete_url, "", headers)
		logger.debug('Server response for collection deletion: (%d) %s' % (response_code, response))
		logger.info("Deleted collection: %s\\%s from %s" % (app, collection, hostname))
		return response_code
	except BaseException as e:
		raise Exception('Failed to delete collection %s/%s from %s: %s' % (app, collection, hostname, repr(e)))

def download_collection(logger, remote_uri, remote_session_key, app, collection, output_file, compress=False):
	# Set request headers
	headers = {
		'Authorization': 'Splunk %s' % remote_session_key,
		'Content-Type': 'application/json'
	}

	# Counters
	loop_record_count = None
	total_record_count = 0

	# Config options
	cfg = cli.getConfStanza('kvstore_tools','settings')
	batch_size = int(cfg.get('backup_batch_size'))
	limits_cfg = cli.getConfStanza('limits','kvstore')
	maxrows = int(limits_cfg.get('max_rows_per_query'))
	url_tmpl_collection_download = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/data/%(collection)s?limit=%(limit)s&skip=%(skip)s&output_mode=json'

	try:
		cursor = 0
		if compress:
			f = gzip.open(output_file, "wb")	# Requires bytes
		else:
			f = open(output_file, "w")			# Requires string

		# If the loop record count is equal to batch size, we hit the limit. Keep going.
		while (loop_record_count is None or loop_record_count == batch_size):

			# Build the URL
			remote_data_url = url_tmpl_collection_download % dict(
				server_uri = remote_uri,
				owner = 'nobody',
				app = app,
				collection = collection,
				limit = batch_size,
				skip = cursor)

			# Download the data from the collection
			response = request('GET', remote_data_url, '', headers)[0]
			response = response.decode('utf-8')
			# Remove the first and last characters ( [ and ] )
			response = response[1:-1]
			# Insert line breaks in between records -- "}, {"
			response = response.replace('}, {', '}, \n{')
			# Count the number of _key values 
			loop_record_count = response.count('"_key"')
			total_record_count += loop_record_count
			logger.debug('Counted %d total records and %d in this loop.' % (total_record_count, loop_record_count))

			# Append the records to the variable
			if loop_record_count > 0:
				## Write the leading [ or comma delimiter (between batches)
				# Start of the collection
				if cursor == 0 and compress:
					f.write('['.encode())
				elif cursor == 0 and not compress:
					f.write('[')

				# Middle of the collection
				elif cursor != 0 and compress:
					f.write(','.encode())
				else:
					f.write(',')
				
				# Response body
				if compress:
					f.write(response.encode())
				else:
					f.write(response)

				# End of the collection
				if loop_record_count < batch_size and compress:
					f.write(']'.encode())
				elif loop_record_count < batch_size and not compress:
					f.write(']')
			cursor += loop_record_count
		f.close()

		logger.debug("Retrieved {0} records from {1}".format(total_record_count, collection))

		if total_record_count > 0:
			if total_record_count == maxrows:
				logger.warning('Downloaded rows equal to configured limit: %s/%s' % (app, collection))
				result = "warning"
				message = "Downloaded rows equal to configured limit. Possible incomplete backup."
			if batch_size > maxrows and total_record_count > maxrows:
				logger.warning('Downloaded KV store collection with batches exceeded the limit: %s/%s' % (app, collection))
				result = "warning"
				message = "Batch size greater than configured query limit. Possible incomplete backup."
			else:
				logger.info('Downloaded KV store collection successfully: %s/%s' % (app, collection))
				result = "success"
				message = "Downloaded collection"
		else:
			logger.debug('Skipping collection: ' + collection)
			result = "skipped"
			message = "Collection is empty"

	except BaseException as e:
		logger.error('Failed to download collection: %s' % repr(e), exc_info=True)
		result = "error"
		message = repr(e)
		total_record_count = 0
		if os.path.isfile(output_file):
			os.remove(output_file)

	return result, message, total_record_count

def upload_collection(logger, remote_uri, remote_session_key, app, collection, file_path):
	# Set request headers
	headers = {
		'Authorization': 'Splunk %s' % remote_session_key,
		'Content-Type': 'application/json'
	}
	
	limits_cfg = cli.getConfStanza('limits','kvstore')
	# !!! This doesn't query the remote host for the limit. Always uses the local instance
	limit = int(limits_cfg.get('max_documents_per_batch_save'))
	logger.debug("Max documents per batch save = %d" % limit)

	try:
		file_name = re.search(r'(.*)(?:\/|\\)([^\/\\]+)', file_path).group(2)

		# Open the file using standard or gzip libs
		if file_path.endswith('.json'):
			fh = open(file_path, 'r')
		elif file_path.endswith('.json.gz'):
			fh = gzip.open(file_path, 'rb')

		# Read the file data and parse with JSON loader
		contents = json.loads(fh.read(), strict=False)
	except BaseException as e:
		# Account for a bug in prior versions where the record count could be wrong if "_key" was in the data and the ] would not get appended.
		logger.error("Error reading file: %s\n\tAttempting modification (Append ']')." % str(e))
		try:
			# Reset the file cursor to 0
			fh.seek(0)
			contents = json.loads(fh.read() + "]", strict=False)
		except BaseException:
			logger.error("[Append ']'] Error reading modified json input.\n\tAttempting modification (Strip '[]')")
			try:
				# Reset the file cursor to 0
				fh.seek(0)
				contents = json.loads(fh.read().strip('[]'), strict=False)
			except BaseException:
				logger.error("[Strip '[]'] Error reading modified json input for file %s.  Aborting." % file_path)
				status = 'error'
				message = 'Unable to read file'
				return status, message, 0

	logger.debug("File read complete.")
	content_len = len(contents)
	logger.debug('File %s entries: %d' % (file_name, content_len))
	'''
	if not append:
		# Delete the collection contents
		try:
			delete_collection(logger, splunkd_uri, session_key, file_app, file_collection)
		except urllib.error.HTTPError as e:
			logger.critical(repr(e))
			yield({'Error': 'Failed to delete collection: %s' % repr(e)})
			sys.exit(4)
	'''
	i = 0
	batch_number = 1
	posted = 0

	# Build the URL for updating the collection

	url_tmpl_batch = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/data/%(collection)s/batch_save?output_mode=json'
	record_url = url_tmpl_batch % dict(
		server_uri = remote_uri,
		owner = 'nobody',
		app = app,
		collection = collection)

	result = None
	while i < content_len:
		# Get the lesser number between (limit-1) and (content_len)
		last = (batch_number*limit)
		last = min(last, content_len)
		batch = contents[i:last]
		i += limit

		logger.debug('Batch number: %d (%d bytes / %d records)' % (batch_number, sys.getsizeof(batch), len(batch)))

		# Upload the restored records to the server
		try:
			response, response_code = request('POST', record_url, json.dumps(batch), headers)		# pylint: disable=unused-variable
			batch_number += 1
			posted += len(batch)
			if response_code != 200:
				raise Exception("Error %d when posting collection contents" % response_code)

		except BaseException as e:
			result = 'error'
			message = 'Failed to upload collection: %s' % repr(e)
			logger.debug(message, exc_info=True)
			#yield {'_time': time.time(), 'app': app, 'kvstore': collection, 'records': posted, 'result': 'error' }
			# Force out of the while loop
			i = content_len
	
	if result is None:
		result = 'success'
		message = "Restored %d records to %s/%s" % (posted, app, collection)
		# Collection now fully restored
		logger.info(message)
	return result, message, posted

def hostname_from_uri(uri):
	return re.sub(r'https?://([^:]+):.*', r'\1', uri)

def parse_custom_credentials(logger, config):
	credentials = {}
	try:
		# Read the splunk.secret file
		with open(os.path.join(os.getenv('SPLUNK_HOME'), 'etc', 'auth', 'splunk.secret'), 'r') as ssfh:
			splunk_secret = ssfh.readline()

		# list all credentials
		for option in config:
			if option[0:10] == 'credential':
				logger.debug('option: ' + str(option))
				config_value = config.get(option)
				logger.debug(config_value)
				try:
					hostname, username, password = config.get(option).split(':')
					credentials[hostname] = {}
					credentials[hostname]['username'] = username
					credentials[hostname]['password'] = decrypt(splunk_secret, password)
				except:
					# Blank or wrong format. Ignore.
					pass
		return credentials
	except Exception as e:
		raise Exception("Could not parse credentials from Splunk. Error: %s" % (str(e)))
