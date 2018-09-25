import unittest
import os
import logging
import six
import json

from pipeutils.avro import Registry, SchemaVersionNotFound
from pipeutils import logger
from avro import schema

path = os.path.dirname(os.path.realpath(__file__))
path_configs = os.path.join(path, 'configs')

_schema = json.dumps({"type": "record", "name": "X", "fields": [{"name": "y", "type": {"type": "record", "name": "Y", "fields": [{"name": "Z", "type": "X"}]}}]})
version = 1
name = 'test'


class TestRegistry(unittest.TestCase):

    def test_get_schema(self):
        """
        Return schema name and version specific and the compare with avro.Parse.
        """
        logger.setLevel(logging.DEBUG)
        logger.info("testing")

        register = Registry(path_configs)
        test = register.get(name, version)
        key = '%s_%s' % (name, str(version))

        logger.info(schema.Parse(test[key]))
        if six.PY3:
            original_parse = schema.Parse(_schema)
            result_parse = schema.Parse(test[key])
        else:
            original_parse = schema.parse(_schema)
            result_parse = schema.parse(test[key])

        self.assertEqual(result_parse, original_parse)

    def test_fail_schema_version(self):
        """
           To test exception raise due to run time error
        """

        logger.setLevel(logging.DEBUG)
        logger.info("testing")
        # not found version 2
        version = 2
        self.register = Registry(path_configs)
        self.assertRaises(SchemaVersionNotFound, lambda: self.register.get(name, version))

if __name__ == '__main__':
    unittest.main()