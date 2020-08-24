from builtins import str
import logging
import json
import splunk.rest as rest

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
	logger = logging.getLogger('kvst')

	# Get system roles
	uri = '/services/admin/roles?output_mode=json&count=-1'
	try:
		serverResponse, serverContent = rest.simpleRequest(uri, sessionKey=sessionKey, method='GET')
		roles_json = json.loads(serverContent)
		if len(roles_json['entry']) > 0:
			for roles_entry in roles_json['entry']:
				role_name = roles_entry["name"]
				system_roles[role_name] = roles_entry["content"]["imported_roles"]
		logger.debug("Roles: {}".format(json.dumps(system_roles)))
	except BaseException as e:
		logger.critical("Error resolving roles: %s", str(e))
		exit(1)

	# Get Splunk users
	uri = '/services/admin/users?output_mode=json&count=-1'
	try:
		serverResponse, serverContent = rest.simpleRequest(uri, sessionKey=sessionKey, method='GET')
	except BaseException as e:
		logger.critical("Could not connect to download role list: %s". str(e))
		exit(1)
	entries = json.loads(serverContent)
	role_users = []
	if len(entries['entry']) > 0:
		for entry in entries['entry']:
			# Only add users with role_alert_manager role
			user_primary_roles = []
			for user_primary_role in entry['content']['roles']:
				logger.debug("User primary role: %s", user_primary_role)
				user_secondary_roles = resolve_roles(user_primary_role, system_roles)
				logger.debug("Resolved user_primary_role {} to {}.".format(user_primary_role, user_secondary_roles))
				user_primary_roles = user_primary_roles + list(set(user_secondary_roles) - set(user_primary_roles))
			logger.debug("Roles of user '%s': %s" % (entry['name'], json.dumps(user_primary_roles)))

			# Check if role is assigned to user
			if role in entry['content']['roles'] or role in user_primary_roles:
				role_users.append( entry['name'] )
			logger.debug("Got list of splunk users for role: %s" % str(role_users))
	return role_users
