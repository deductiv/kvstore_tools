# roles.py
# Helper to query roles list for a given user
# Validate authorization 

from builtins import str
import logging
import json
import splunk.rest as rest
from deductiv_helpers import eprint

system_roles = {}
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
