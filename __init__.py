import os
import six
import errno


if six.PY3:
    import configparser as ConfigParser
else:
    import ConfigParser

HOME = os.path.expanduser("~")
CONFIG_PATH = os.environ.get('PIPE_CONFIG_PATH', '~/.pipeutils/config/')


def config(name='base'):
    '''
    Args:
        name (str): The `name` is name of config file.
    Returns:
        (dict) a configuration read from a file found in CONFIG_PATH
    '''

    if name == 'base':
        paths = [
            os.environ.get('PIPE_CONFIG_PATH', None),
            os.path.join(HOME, '.pipeutils', 'config', 'default.conf'),
            os.path.join(CONFIG_PATH, 'default.conf'),
        ]
    elif name == 'aws':
        print('into')
        paths = [
            os.path.join(HOME, '.pipeutils', 'config', 'aws.conf'),
            os.path.join(CONFIG_PATH, 'aws.conf'),
        ]

    else:
        raise ValueError("Failed not open for {!r})".format(name))

    path = None
    for p in paths:
        if p is not None:
            if os.path.exists(p):
                path = p
                break

    if path is None:
        raise Exception("Config file default.conf couldn't be found")

    if six.PY3:
        config = ConfigParser.SafeConfigParser()
    else:
        config = ConfigParser.ConfigParser()

    config.read([path])
    configuration = dict(config.items(name))

    return configuration


PIPE_PATH = os.path.os.path.dirname(__file__)
