import import_declare_test
import sys
import os
import random
import logging
from logging.handlers import RotatingFileHandler
from splunk import admin

connection_type_auths = {
    'splunk': ['api_key', 'password'],
    's3': ['aws_secret', 'aws_assumed_role'],
    'azureblob': ['azure_storage', 'azure_ad'],
    'sftp': ['password', 'private_key'],
    'smb': ['password', 'domain_account']
}

def str2bool(val):
    """ Convert a string value to a boolean value """
    if isinstance(val, bool):
        return val
    return str(val).lower() in ("yes", "y", "true", "t", "1")

class ConfigApp(admin.MConfigHandler):
    def setup(self):
        self.supportedArgs.addOptArg('connection_type')
    
    def handleList(self, confInfo):
        # Facility info - prepended to log lines
        facility = os.path.basename(__file__)
        facility = os.path.splitext(facility)[0]
        # Return a complete standalone logging module with a given name
        random_number = str(random.randint(10000, 100000))
        filename = 'kvstore_tools.log'
        logger = logging.getLogger(filename + str(random_number))
        # Prevent the log messages from being duplicated in the python.log file
        logger.propagate = False
        logger.setLevel('DEBUG')

        log_file = os.path.join(os.environ['SPLUNK_HOME'], 'var', 'log', 'splunk', filename)
        file_handler = RotatingFileHandler(log_file, maxBytes=25000000, backupCount=2)
        formatter = logging.Formatter(f'%(asctime)s [{facility}] %(levelname)s %(message)s')
        file_handler.setFormatter(formatter)
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)
        logger.addHandler(stderr_handler)
        logger.debug('KV Store Tools Settings handler started (List)')
        
        conf = self.readConf("kvstore_tools_account")
        logger.debug(self.callerArgs.data['connection_type'][0])

        if conf is not None:
            for stanza, settings in list(conf.items()):
                for key, val in list(settings.items()):
                    if key == 'auth_type' \
                        and val in list(connection_type_auths[self.callerArgs.data['connection_type'][0]]) \
                        and not str2bool(conf[stanza]['disabled']):
                        confInfo[stanza].append(key, val)

# initialize the handler
admin.init(ConfigApp, admin.CONTEXT_NONE)
