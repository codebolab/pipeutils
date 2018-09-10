import os
import sys

if sys.version_info.major == 2:
    import ConfigParser
else:
    import configparser as ConfigParser

HOME = os.path.expanduser("~")


def config(section='default'):
    paths = [
        os.environ.get('PIPE_CONFIG_PATH', None),
        os.path.join(HOME, '.pipe', 'default.conf'),
    ]
    path = None
    if section == 'aws':
        paths = [
            os.path.join(HOME, '.pipe', 'aws.conf'),
        ]

    for p in paths:
        if p is not None:
            if os.path.exists(p):
                path = p
                break

    if path is None:
        raise Exception("Config file default.conf couldn't be found")

    if sys.version_info.major > 2 and sys.version_info.minor > 3:
        config = ConfigParser.ConfigParser()
    else:
        config = ConfigParser.SafeConfigParser()

    config.read([path])
    configuration = dict(config.items(section))

    return configuration


PIPE_PATH = os.path.os.path.dirname(__file__)
