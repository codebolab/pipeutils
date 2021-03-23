import os

PATH = os.path.dirname(os.path.realpath(__file__))
PATH_REGISTRY = os.path.join(PATH, 'registry')
PATH_CONFIG = os.path.join(PATH, 'configs')

os.environ.setdefault('PIPE_SCHEMA_REGISTRY', PATH_REGISTRY)
os.environ.setdefault('PIPE_CONFIG_PATH', PATH_CONFIG)
