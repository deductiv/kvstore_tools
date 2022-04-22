# Common cross-app functions to simplify code

# Author: J.R. Murray <jr.murray@deductiv.net>
# Version: 2.0.4

from __future__ import print_function
from builtins import str
from future import standard_library
standard_library.install_aliases()
import sys, os
import urllib.request, urllib.parse, urllib.error
import re
import logging
import configparser

# Add lib folders to import path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'lib'))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib'))

# pylint: disable=import-error
import splunk.entity as en 

def get_credentials(app, session_key):
	try:
		# list all credentials
		entities = en.getEntities(['admin', 'passwords'], namespace=app,
									owner='nobody', sessionKey=session_key)
	except Exception as e:
		raise Exception("Could not get %s credentials from Splunk. Error: %s" % (app, str(e)))

	credentials = []
	
	for id, c in list(entities.items()):		# pylint: disable=unused-variable
		# c.keys() = ['clear_password', 'password', 'username', 'realm', 'eai:acl', 'encr_password']
		if c['eai:acl']['app'] == app:
			credentials.append( {'realm': c["realm"], 'username': c["username"], "password": c["clear_password"] } )
	
	if len(credentials) > 0:
		return credentials
	else:
		raise Exception("No credentials have been found")

# HTTP request wrapper
def request(method, url, data, headers):
	"""Helper function to fetch data from the given URL"""
	# See if this is utf-8 encoded already
	try:
		data.decode('utf-8')
	except:
		try:
			data = urllib.parse.urlencode(data).encode("utf-8")
		except:
			data = data.encode("utf-8")
	req = urllib.request.Request(url, data, headers)
	req.get_method = lambda: method
	res_txt = ""
	res_code = "0"
	try: 
		res = urllib.request.urlopen(req)
		res_txt = res.read()
		res_code = res.getcode()
	except urllib.error.HTTPError as e:
		res_code = e.code
		res_txt = e.read()
		eprint("HTTP Error: " + str(res_txt))
	except BaseException as e:
		eprint("URL Request Error: " + str(e))
		sys.exit(1)
	return res_txt, res_code

def setup_logging(logger_name):
	logger = logging.getLogger(logger_name)
	return logger

# For alert actions
def setup_logger(level, filename, facility):
	logger = logging.getLogger(filename)
	# Prevent the log messages from being duplicated in the python.log file
	logger.propagate = False 
	logger.setLevel(level)
	
	log_file = os.path.join( os.environ['SPLUNK_HOME'], 'var', 'log', 'splunk', filename )
	file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=25000000, backupCount=2)
	formatter = logging.Formatter('%(asctime)s [{0}] %(levelname)s %(message)s'.format(facility))
	file_handler.setFormatter(formatter)
	logger.addHandler(file_handler)
	
	return logger

def read_config(filename):
	config = configparser.ConfigParser()
	app_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
	app_child_dirs = ['default', 'local']
	for cdir in app_child_dirs:
		try:
			config_file = os.path.join( app_dir, cdir, filename )
			config.read(config_file)
		except:
			pass
	return config

# Merge two dictionary objects (x,y) into one (z)
def merge_two_dicts(x, y):
	z = x.copy()	# start with x's keys and values
	z.update(y)	# modifies z with y's keys and values & returns None
	return z

def hex_convert(s):
	return ":".join("{:02x}".format(ord(c)) for c in s)

def str2bool(v):
	return v.lower() in ("yes", "true", "t", "1")

# STDERR printing for python 3
def eprint(*args, **kwargs):
	print(*args, file=sys.stderr, **kwargs)

def escape_quotes(string):
	string = re.sub(r'(?<=\\)"', r'\\\"', string)
	string = re.sub(r'(?<!\\)"', r'\"', string)
	return string

def is_ipv4(host):
	r = "^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$"
	if(re.search(r, host)):
		return True
	else:
		return False
