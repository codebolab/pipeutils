import os

HOME = os.path.expanduser("~")
VERSION = 1



class Registry:

    '''
        A schema registry should manage the avro schemas and versions.
    '''
    def __init__(self, path=None):
        self.schema = {}
        if path is not None:
            self.path = path
        elif os.environ.get('PIPE_SCHEMA_REGISTRY'):
            self.path = os.environ.get('PIPE_SCHEMA_REGISTRY')
        else:
            self.path = os.path.join(HOME, '.pipeutils', 'registry')

    def _cache(self, key, value):
        # overwrite, not much performance impact, as shouldn't be happening often
        self.schema[key] = value

    def get(self, name=None, version=VERSION):
        _schema = os.path.join(self.path, name)
        if os.path.exists(_schema):
            for f in os.listdir(_schema):
                if f.endswith('.avsc') and int(os.path.splitext(f)[0]) == version:
                    with open(os.path.join(self.path, name, f)) as a:
                        key = '%s_%s' % (name, str(version))
                        self._cache(key, a.read())
                else:
                    raise SchemaVersionNotFound
        else:
            raise SchemaNotFound
        return self.schema


class SchemaNotFound(Exception):
    """
    Exception when an schema couldn't be found
    """
    pass


class SchemaVersionNotFound(Exception):
    """
    Exception when an schema version couldn't be found
    """
    pass
