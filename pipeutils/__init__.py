import os
import six
import logging
import configparser as ConfigParser
from pipeutils import exceptions

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
HOME = os.path.expanduser("~")


def config(name='base', path=None):
    '''
    Args:
        name (str): The `name` is name of config file.
        dir_path (str): The dir_path is environ or config file dir
    Returns:
        (dict) a configuration read from a file found in CONFIG_PATH
    '''

    if path is None:
        path = os.environ.get(
            'PIPE_CONFIG_PATH',
            os.path.join(HOME, '.pipeutils', 'config')
        )

    # check file exists
    path_file = os.path.join(path, '%s.conf' % name)
    logger.info('Config File: %s' % path_file)
    if os.path.exists(os.path.join(path, path_file)):
        path = os.path.join(path, path_file)
    else:
        raise exceptions.ConfigNotFound()

    if six.PY3:
        config = ConfigParser.ConfigParser()
    else:
        config = ConfigParser.SafeConfigParser()

    config.read([path])
    configuration = dict(config.items(name))

    return configuration


PIPE_PATH = os.path.os.path.dirname(__file__)
