import os
import avro.schema

HOME = os.path.expanduser("~")
VERSION = 1


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


class Registry:

    '''
        A schema registry should manage the avro schemas and versions.
    '''
    def __init__(self, path=None):
        self.cache_schemas = {}
        if path is not None:
            self.path = path
        elif os.environ.get('PIPE_SCHEMA_REGISTRY'):
            self.path = os.environ.get('PIPE_SCHEMA_REGISTRY')
        else:
            self.path = os.path.join(HOME, '.pipeutils', 'registry')

    def _cache(self, key, value):
        # overwrite, not much performance impact, as shouldn't be happening often
        self.cache_schemas[key] = value
        
    def get(self, name=None, version=VERSION):
        _schema = os.path.join(self.path, name)
        if os.path.exists(_schema):
            for dirpath, dirs, files in os.walk(_schema):    
                avro_file = '%s.avsc' % version
                print("Files %s" % sorted(files))
                if avro_file in sorted(files):
                    _file = os.path.join(self.path, name, avro_file)
                    return avro.schema.Parse(open(_file, "rb").read())
                    with open(os.path.join(self.path, name, avro_file)) as a:
                        key = '%s_%s' % (name, str(version))
                        self._cache(key, a.read())
                        break 
                else:
                    raise SchemaVersionNotFound
        else:
            raise SchemaNotFound

registry = Registry()
