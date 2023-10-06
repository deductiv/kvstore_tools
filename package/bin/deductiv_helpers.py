"""
Common cross-app functions to simplify code
Version 2.0.9 (2023-02-15)
"""

import sys
import os
import urllib.request
import urllib.parse
import urllib.error
import http.client as httplib
import ssl
import re
import logging
import configparser
import time
import datetime
import socket
import json
import random
import struct
import splunk
import splunk.entity as en
from splunk.rest import simpleRequest

# Add lib folders to import path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'lib'))
# pylint: disable=wrong-import-position, import-error
from splunksecrets import decrypt

def get_credentials(app, session_key):
	""" Get an app's credentials from the Splunk REST API """
	try:
		# list all credentials
		entities = en.getEntities(['admin', 'passwords'], namespace=app,
          owner='nobody', sessionKey=session_key)
	except Exception as e:
		raise Exception(f"Could not get {app} credentials from Splunk. Error: {str(e)}") from e

	credentials = []

	for entry in list(entities.values()):		# pylint: disable=unused-variable
		# c.keys() = ['clear_password', 'password', 'username', 'realm', 'eai:acl', 'encr_password']
		if entry['eai:acl']['app'] == app:
			credentials.append({'realm': entry["realm"],
			  'username': entry["username"],
			  "password": entry["clear_password"]})

	if len(credentials) > 0:
		return credentials
	else:
		raise Exception("No credentials have been found")

# HTTP request wrapper
def request(method, url, data, headers, conn=None, verify=None):
	"""Helper function to fetch data from the given URL"""
	# See if this is utf-8 encoded already
	try:
		data.decode('utf-8')
	except AttributeError:
		try:
			data = urllib.parse.urlencode(data).encode("utf-8")
		except TypeError:
			data = data.encode("utf-8")
	url_tuple = urllib.parse.urlparse(url)
	if conn is None:
		close_conn = True
		if url_tuple.scheme == 'https':
			ctx = ssl.create_default_context()
			# If verify was set explicitly, OR it's not set to False and env[PYTHONHTTPSVERIFY] is set
			env_verify_set = os.environ.get('PYTHONHTTPSVERIFY', default=True)
			if verify is False or (env_verify_set is False and not verify):
				ctx.check_hostname = False
				ctx.verify_mode = ssl.CERT_NONE
			conn = httplib.HTTPSConnection(url_tuple.netloc, context=ctx)
		elif url_tuple.scheme == 'http':
			conn = httplib.HTTPConnection(url_tuple.netloc)
	else:
		close_conn = False
	try:
		conn.request(method, url, data, headers)
		response = conn.getresponse()
		response_data = response.read()
		response_status = response.status
		if close_conn:
			conn.close()
		return response_data, response_status
	except Exception as e:
		raise Exception(f"URL Request Error: {repr(e)}") from e

def setup_logging(logger_name):
	""" Return the logging module with a given name """
	logger = logging.getLogger(logger_name)
	return logger

# For alert actions
def setup_logger(level, filename, facility):
	""" Return a complete standalone logging module with a given name """
	random_number = str(random.randint(10000, 100000))
	logger = logging.getLogger(filename + str(random_number))
	# Prevent the log messages from being duplicated in the python.log file
	logger.propagate = False 
	logger.setLevel(level)

	log_file = os.path.join(os.environ['SPLUNK_HOME'], 'var', 'log', 'splunk', filename)
	file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=25000000, backupCount=2)
	formatter = logging.Formatter('%(asctime)s [{0}] %(levelname)s %(message)s'.format(facility))
	file_handler.setFormatter(formatter)
	stderr_handler = logging.StreamHandler(sys.stderr)
	stderr_handler.setLevel(logging.ERROR)
	logger.addHandler(file_handler)
	logger.addHandler(stderr_handler)

	return logger

def read_config(filename):
	""" Read a configuration file on disk and return the configuration object """
	config = configparser.ConfigParser()
	app_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
	app_child_dirs = ['default', 'local']
	for cdir in app_child_dirs:
		try:
			config_file = os.path.join(app_dir, cdir, filename)
			config.read(config_file)
		except Exception:
			pass
	return config

def merge_two_dicts(x, y):
	""" Merge two dictionary objects (x,y) into one (z) """
	z = x.copy()	# start with x's keys and values
	z.update(y)	# modifies z with y's keys and values & returns None
	return z

def hex_convert(s):
	""" Convert a string to hexadecimal format """
	return ":".join("{:02x}".format(ord(c)) for c in s)

def str2bool(v):
	""" Convert a string value to a boolean value """
	if isinstance(v, bool):
		return v
	else:
		return str(v).lower() in ("yes", "y", "true", "t", "1")

# STDERR printing for python 3
def eprint(*args, **kwargs):
	""" Print output to stderr; helpful for writing to search.log """
	print(*args, file=sys.stderr, **kwargs)

def escape_quotes(string):
	""" Escape quotation marks in strings """
	# Replace " preceded by \ (double escape)
	string = re.sub(r'(?<=\\)"', r'\\\"', string)
	# Replace " not preceded by \
	string = re.sub(r'(?<!\\)"', r'\"', string)
	return string

def escape_quotes_csv(string):
	""" Double-escape quotes for CSV output """
	return string.replace('"', '""')

def is_ipv4(host):
	""" Check if a string is an IPv4 address """
	regex = r'^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$'
	return bool(re.search(regex, host))

def replace_keywords(input_string):
	""" Replace keywords in strings for use with scheduled file outputs """
	now = str(int(time.time()))
	nowms = str(int(time.time()*1000))
	nowft = datetime.datetime.now().strftime("%F_%H%M%S")
	today = datetime.datetime.now().strftime("%F")
	yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%F")

	keyword_replacements = {
		'__now__': now,
		'__nowms__': nowms,
		'__nowft__': nowft,
		'__today__': today,
		'__yesterday__': yesterday
	}

	for keyword_input in list(keyword_replacements.keys()):
		input_string = input_string.replace(keyword_input, keyword_replacements[keyword_input])
	return input_string

class SearchConsole:
	""" Object to help interface with search UI (e.g. print job errors) """
	def __init__(self, logger, caller_object):
		self.logger = logger
		self.caller_object = caller_object

	def exit_error(self, message, error_code=1):
		""" Exit with an error; echo to the search console and the log file first """
		eprint(message)
		if self.caller_object is not None:
			if hasattr(self.caller_object, '_configuration'):
				command = str(self.caller_object._configuration.command).split(' ', maxsplit=1)[0] #pylint: disable=protected-access
			else:
				command = ''
			if hasattr(self.caller_object, 'write_error'):
				self.caller_object.write_error(f'{command}: {message}')
		self.logger.critical(message)
		sys.exit(error_code)

def decrypt_with_secret(encrypted_text):
	""" Use splunksecrets.py and splunk.secret to decrypt a secret """
	# Check for encryption
	if encrypted_text[:1] == '$':
		# Decrypt the text
		# Read the splunk.secret file
		with open(os.path.join(os.getenv('SPLUNK_HOME'), 'etc', 'auth', 'splunk.secret'), 'r', encoding='ascii') as ssfh:
			splunk_secret = ssfh.readline()
		# Call the decrypt function from splunksecrets.py
		return decrypt(splunk_secret, encrypted_text)
	else:
		# Not encrypted
		return encrypted_text

def port_is_open(ip_address, port):
	""" Connect to a host and check if the port is open """
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.settimeout(3)
	try:
		sock.connect((ip_address, int(port)))
		sock.shutdown(2)
		return True
	except Exception:
		return False

def get_tokens(searchinfo):
	""" Read token keywords ($keyword$) from search job properties """
	tokens = {}
	# Get the host of the splunkd service
	splunkd_host = searchinfo.splunkd_uri[searchinfo.splunkd_uri.index("//")+2:searchinfo.splunkd_uri.rindex(":")]
	splunkd_port = searchinfo.splunkd_uri[searchinfo.splunkd_uri.rindex(":")+1:]

	tokens = {
		'splunkd_host': splunkd_host,
		'splunkd_port': splunkd_port
	}

	# Get the search job attributes
	if searchinfo.sid:
		job_uri = en.buildEndpoint(
			[
				'search', 
				'jobs', 
				searchinfo.sid
			], 
			namespace = searchinfo.app,
			owner = searchinfo.owner
		)
		try:
			job_response = simpleRequest(job_uri, getargs={'output_mode':'json'}, sessionKey=searchinfo.session_key)[1]
			search_job = json.loads(job_response)
			job_content = search_job['entry'][0]['content']
		except splunk.ResourceNotFound:
			# This probably ran on the indexer and failed
			job_content = {}
	else:
		job_content = {}

	#eprint("job_content=" + json.dumps(job_content))
	for key, value in list(job_content.items()):
		if value is not None:
			tokens['job.' + key] = json.dumps(value, default=lambda o: o.__dict__)

	if 'label' in list(job_content.keys()):
		tokens['name'] = job_content['label']

		# Get the saved search properties
		entity_class = ['saved', 'searches']
		uri = en.buildEndpoint(
			entity_class,
			namespace=searchinfo.app, 
			owner=searchinfo.owner
		)

		response_body = simpleRequest(uri, getargs={'output_mode':'json'}, sessionKey=searchinfo.session_key)[1]

		saved_search = json.loads(response_body)
		ss_content = saved_search['entry'][0]['content']
		#eprint("SSContent=" + json.dumps(ss_content))

		for key, value in list(ss_content.items()):
			if not key.startswith('display.'):
				if value is not None:
					tokens[key] = json.dumps(value, default=lambda o: o.__dict__)

	tokens['owner'] = searchinfo.owner
	tokens['app'] = searchinfo.app
	#tokens['results_link'] = 'http://127.0.0.1:8000/en-US/app/search/search?sid=1622650709.10799'

	# Parse all of the nested objects (recursive function)
	for t, tv in list(tokens.items()):
		tokens = merge_two_dicts(tokens, parse_nested_json(t, tv))

	#for t, tv in list(tokens.items()):
	#	if type(tv) == str:
	#		eprint(t + '=' + tv)
	#	else:
	#		eprint(t + "(type " + str(type(tv)) + ") = " + str(tv))
	return tokens

def parse_nested_json(parent_name, json_string):
	""" Parse the nested json data within strings """
	retval = {}
	try:
		if json_string is not None:
			json_object = json.loads(json_string)
			if json_object is not None:
				for key, value in list(json_object.items()):
					if isinstance(value, dict):
						retval = merge_two_dicts(retval, parse_nested_json(parent_name + '.' + key, json.dumps(value)))
					else:
						retval[(parent_name + '.' + key).replace('..', '.')] = value
						#eprint('added subtoken ' + (parent_name + '.' + u).replace('..', '.') + '=' + str(uv))
		return retval
	except ValueError:
		return {parent_name: json_string}
	except AttributeError:
		return {parent_name: json_string}
	except Exception as e:
		eprint("Exception parsing JSON subtoken: " + repr(e))
	
def replace_object_tokens(search_command_object):
	""" Replace tokenized strings in properties of a search command object """
	tokens = get_tokens(search_command_object._metadata.searchinfo)
	for var in vars(search_command_object):
		val = getattr(search_command_object, var)
		try:
			if '$' in val:
				try:
					setattr(search_command_object, var, replace_string_tokens(tokens, val))
				except Exception as e:
					eprint("Error replacing token text for variable %s value %s: %s" % (var, val, repr(e)))
		except Exception:
			# Probably an index out of range error
			pass
	#return o

	#for t, v in list(tokens.items()):
	#	param = param.replace('$'+t+'$', v)
	#return param

def replace_string_tokens(tokens, tokenized_string):
	""" Replace tokens ($token$) in tokenized strings """
	for token, token_value in list(tokens.items()):
		if token_value is not None:
			tokenized_string = tokenized_string.replace('$'+token+'$', str(token_value).strip('"').strip("'"))
	# Print the result if the value changed
	#if b != v:
	#	eprint(b + ' -> ' + v)
	return tokenized_string

def recover_parameters(obj):
	""" If arguments are not set for our command, get them manually from the command string """
	args = sys.argv[2:]
	for a in args:
		key = a[0:a.index('=')].strip('"')
		value = a[a.index('=')+1:].strip('"')
		if value != '__default__':
			setattr(obj, key, value)
		else:
			setattr(obj, key, None)

def log_proxy_settings(logger):
	""" Enumerate and log the environment's proxy settings """
	# Enumerate proxy settings
	http_proxy = os.environ.get('HTTP_PROXY')
	https_proxy = os.environ.get('HTTPS_PROXY')
	proxy_exceptions = os.environ.get('NO_PROXY')

	if http_proxy is not None:
		logger.debug("HTTP proxy: %s" % http_proxy)
	if https_proxy is not None:
		logger.debug("HTTPS proxy: %s" % https_proxy)
	if proxy_exceptions is not None:
		logger.debug("Proxy Exceptions: %s" % proxy_exceptions)

def is_cloud(session_key):
	""" Check to see if this host is running on Splunk Cloud """
	uri = en.buildEndpoint(["server", "info", "server-info"], namespace='-', owner='nobody')
	server_content = simpleRequest(uri, getargs={"output_mode": "json"}, sessionKey=session_key, raiseAllErrors=True)[1]
	try:
		# Test for non-cloud environment
		#return json.loads(server_content)['entry'][0]['content']['federated_search_enabled'] # true
		# See if instance_type is set to "cloud"
		instance_type = json.loads(server_content)['entry'][0]['content']['instance_type']
		return instance_type == "cloud"
	except KeyError:
		return False

def get_uncompressed_size(filename):
	""" Get the uncompressed size of a .gz file """
	with open(filename, 'rb') as f:
		f.seek(-4, 2)
		return struct.unpack('I', f.read(4))[0]

if __name__ == "__main__":
	# pylint: disable=unnecessary-pass
	pass
