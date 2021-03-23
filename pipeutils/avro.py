import os
import avro.schema
from pipeutils import logger

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
    """
    A schema registry should manage the avro schemas and versions.
    """

    def __init__(self, path=None):
        logger.info(f"++ Registry.init")
        self.cache_schemas = {}

        if path is not None:
            self.path = path
        elif os.environ.get('PIPE_SCHEMA_REGISTRY'):
            self.path = os.environ.get('PIPE_SCHEMA_REGISTRY')
        else:
            self.path = os.path.join(HOME, '.pipeutils', 'registry')

        logger.info(f"   path: {os.environ.get('PIPE_SCHEMA_REGISTRY')}")
        logger.info(f"   path: {self.path}")

    def _cache(self, key, value):
        # overwrite, not much performance impact, as shouldn't be happening often
        self.cache_schemas[key] = value

    def get(self, name=None, version=VERSION):
        key = '%s_%s' % (name, str(version))
        _schema = os.path.join(self.path, name)

        if key in self.cache_schemas:
            logger.debug('key : %s ', key)
            return self.cache_schemas[key]

        if os.path.exists(_schema):
            element = os.listdir(_schema)  
            avro_file = '%s.avsc' % version
            logger.info("Files - > %s" % sorted(element))
            if avro_file in sorted(element):
                _file = os.path.join(self.path, name, avro_file)

                try:
                    with open(_file, 'rb') as f:
                        data = f.read()
                    # logger.info(data)
                    self._cache(key, avro.schema.Parse(data))
                    return avro.schema.Parse(data)
                except IOError as e:
                    logger.warning("See exception below; skipping file %s", _file)
                    logger.exception(e)
            else:
                raise SchemaVersionNotFound
        else:
            raise SchemaNotFound


registry = Registry()
