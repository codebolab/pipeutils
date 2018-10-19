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
            #dirpath, dirs,
            files = os.walk(_schema)  
            element = [f for d, p, f in files]
            avro_file = '%s.avsc' % version
            print("Files %s" % sorted(element[0]))
            if avro_file in sorted(element[0]):
                _file = os.path.join(self.path, name, avro_file)
                key = '%s_%s' % (name, str(version))
                with open(_file, 'rb') as f:
                    data = f.read()
                self._cache(key, avro.schema.Parse(data))
                return avro.schema.Parse(data)
            else:
                raise SchemaVersionNotFound
        else:
            raise SchemaNotFound

registry = Registry()
