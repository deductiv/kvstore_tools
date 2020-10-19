# kv_collection_copy.py
# Functions for managing KV Store collection data

from builtins import str
import json
import logging
import splunk.rest as rest
from splunk.clilib import cli_common as cli
from deductiv_helpers import eprint, request, setup_logging

def get_server_apps(uri, session_key, app = None):
	apps = []
	if app is not None:
		apps.append(app)
	else:
		# Enumerate all remote apps
		apps_uri = uri + '/services/apps/local?output_mode=json'
		response, content = rest.simpleRequest(apps_uri, sessionKey=session_key, method='GET')
		content = json.loads(content)
		for entry in content["entry"]:
			if not entry["content"]["disabled"]:
				apps.append(entry["name"])
	return apps

def enumerate_collections(uri, session_key, collection, app, app_list, global_scope):
	url_tmpl_app = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/config?output_mode=json&count=0'

	# Enumerate all collections in the apps list
	collections = []
	for app in app_list:
		#logger.debug("%s Polling collections in app: %s" , facility, app)
		# Enumerate all of the collections in the app (if an app is selected)
		collections_url = url_tmpl_app % dict(
			server_uri=uri,
			owner='nobody',
			app=app)
		headers = {
			'Authorization': 'Splunk %s' % session_key,
			'Content-Type': 'application/json'}
		fetch_errors = []
		try:
			response = request('GET', collections_url, '', headers)
		except BaseException as e:
			fetch_errors.append(repr(e))

		if len(fetch_errors) == 0:
			for entry in response["entry"]:
				entry_app = entry["acl"]["app"]
				entry_collection = entry["name"]
				entry_sharing = entry["acl"]["sharing"]

				if ((app == entry_app and collection == entry_collection) or 
				  (app is None and collection == entry_collection) or
				  (app == entry_app and collection is None) or
				  (entry_sharing == 'global' and global_scope) or 
				  (app is None and collection is None)):
					c = [entry_app, entry_collection]
					if c not in collections:
						collections.append(c)
			return collections
		else:
			raise Exception('Errors encountered when filtering collection list: ' + str(fetch_errors))

def copy_collection(logger, source_uri, target_uri, app, collection, append):
	url_tmpl_collection_download = '%(server_uri)s/servicesNS/%(owner)s/%(app)s/storage/collections/data/%(collection)s?limit=%(limit)s&skip=%(skip)s&output_mode=json'
	# Enumerate all of the collections in the app (if an app is selected)
	collection_contents = download_collection(source_uri, app, collection)

	# Download the collection

	# Delete the remote collection (if overwrite)
	if not append:
		pass

	# Upload the collection


def download_collection(uri, app, collection):
	logger = setup_logging('kvst')
	# Reset every iteration to remote splunk uri
	headers = {
		'Authorization': 'Splunk %s' % remote_session_key,
		'Content-Type': 'application/json'}
	batched_response = ''

	# Counters
	loop_record_count = None
	total_record_count = 0
	message = None
	limits_cfg = cli.getConfStanza('limits','kvstore')
	maxrows = int(limits_cfg.get('max_rows_per_query'))
	logger.debug('%s maxrows per query: %s', facility, str(maxrows))
	logger.debug('%s Collection: %s', facility, collection)

	try:
		cursor = 0

		# If the loop record count is equal to batch size, we hit the limit. Keep going.
		while (loop_record_count is None or loop_record_count == batch_size):

			# Build the URL
			remote_data_url = url_tmpl_collection % dict(
				server_uri=remote_uri,
				owner='nobody',
				app=collection_app,
				collection=collection_name,
				limit = batch_size,
				skip = cursor)

			# Download the data from the collection
			response = request('GET', remote_data_url, '', headers)

			# Remove the first and last characters ( [ and ] )
			#response = json.dumps(response)[1:-1]
			#logger.debug('Response: %s ' , response)
			loop_record_count = response.count('_key')
			total_record_count += loop_record_count
			logger.debug('%s Counted %d total records and %d in this loop.', facility, total_record_count, loop_record_count)

			# Append the records to the variable
			if loop_record_count > 0:
				## Write the leading [ or comma delimiter (between batches)
				if cursor == 0:
					batched_response = batched_response + '['
				else:
					batched_response = batched_response + ','
				batched_response = batched_response + response
				if loop_record_count < batch_size:
					batched_response = batched_response + ']'
			cursor += loop_record_count

		logger.debug("%s Retrieved {0} records from {1}".format(total_record_count, collection_name), facility)

		if total_record_count > 0:
			if total_record_count == maxrows:
				logger.warning('%s Downloaded rows equal to configured limit: %s/%s', facility, entry_app, collection_name)
				result = "warning"
				message = "Downloaded rows equal to configured limit. Possible incomplete backup."
			if batch_size > maxrows and total_record_count > maxrows:
				logger.warning('%s Downloaded KV store collection with batches exceeded the limit: %s/%s', facility, entry_app, collection_name)
				result = "warning"
				message = "Batch size greater than configured query limit. Possible incomplete backup."
			else:
				logger.info('%s Downloaded KV store collection successfully: %s/%s', facility, entry_app, collection_name)
				result = "success"
				message = "Downloaded collection"
			# make it a json object
			batched_response = json.loads(batched_response)
		else:
			logger.debug('Skipping collection: ' + collection_name)
			result = "skipped"
			message = "Collection is empty"

	except BaseException as e:
		logger.error('%s Failed to download collection: %s', facility, str(e))
		result = "error"
		message = str(e)
		total_record_count = 0
		errors.append('%s Error downloading collection: ' + collection_app + '/' + collection_name, facility)



def resolve_roles(role, roles):
	if role in roles:
		inherited_roles = roles[role]
	else:
		inherited_roles = []

	inherited_roles.append(role)
	for inherited_role in inherited_roles:
		if inherited_role != role:
			new_roles = resolve_roles(inherited_role, roles)
			if len(new_roles) > 0:
				inherited_roles = inherited_roles + list(set(new_roles) - set(inherited_roles))

	return inherited_roles

def get_role_users(sessionKey, role):
	system_roles = {}
	# Get system roles
	uri = '/services/admin/roles?output_mode=json&count=-1'
	try:
		serverResponse, serverContent = rest.simpleRequest(uri, sessionKey=sessionKey, method='GET')
		roles_json = json.loads(serverContent)
		if len(roles_json['entry']) > 0:
			for roles_entry in roles_json['entry']:
				role_name = roles_entry["name"]
				system_roles[role_name] = roles_entry["content"]["imported_roles"]
		#logger.debug("Roles: {}".format(json.dumps(system_roles)))
	except BaseException as e:
		raise Exception("Error resolving roles: %s", repr(e))

	# Get Splunk users
	uri = '/services/admin/users?output_mode=json&count=-1'
	try:
		serverResponse, serverContent = rest.simpleRequest(uri, sessionKey=sessionKey, method='GET')
	except BaseException as e:
		raise Exception("Could not connect to download role list: %s", repr(e))
		
	entries = json.loads(serverContent)
	role_users = []
	if len(entries['entry']) > 0:
		for entry in entries['entry']:
			# Only add users with role_alert_manager role
			user_primary_roles = []
			for user_primary_role in entry['content']['roles']:
				user_secondary_roles = resolve_roles(user_primary_role, system_roles)
				user_primary_roles = user_primary_roles + list(set(user_secondary_roles) - set(user_primary_roles))
			eprint("Roles of user '%s': %s" % (entry['name'], json.dumps(user_primary_roles)))

			# Check if role is assigned to user
			if role in entry['content']['roles'] or role in user_primary_roles:
				role_users.append( entry['name'] )
			eprint("Got list of splunk users for role: %s" % str(role_users))
	return role_users
