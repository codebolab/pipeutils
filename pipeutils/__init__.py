import os
import six
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

if six.PY3:
    import configparser as ConfigParser
else:
    import ConfigParser

HOME = os.path.expanduser("~")
CONFIG_PATH = os.environ.get('PIPE_CONFIG_PATH', os.path.join(HOME, '.pipeutils', 'config'))


def config(name='base', dir_path=CONFIG_PATH):
    '''
    Args:
        name (str): The `name` is name of config file.
    Returns:
        (dict) a configuration read from a file found in CONFIG_PATH
    '''

    path = None

    # check file exists
    _file = os.path.join(dir_path, '%s.conf' % name)
    if os.path.exists(os.path.join(dir_path, _file)):
        path = os.path.join(dir_path, _file)
    else:
        raise Error("Failed not open for {!r})".format(name), 'ConfigNotFound')

    #if path is None:
        #raise Error("Failed not open for {!r})".format(name), 'ConfigNotFound')

    if six.PY3:
        config = ConfigParser.ConfigParser()
    else:
        config = ConfigParser.SafeConfigParser()

    config.read([path])
    configuration = dict(config.items(name))

    return configuration


class Error(Exception):
   """Base class for other exceptions"""
   def __init__(self, message, errors):
       #if six.PY3:
       #    super().__init__(message)
       #else:
       super(Error, self).__init__(message)
       self.errors = errors

class ValueError(Error):
   """Raised when the input value"""
   pass


PIPE_PATH = os.path.os.path.dirname(__file__)
