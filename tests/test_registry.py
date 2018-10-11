import unittest
import os
import logging
import six
import json

from pipeutils.avro import Registry, SchemaVersionNotFound, SchemaNotFound
from pipeutils import logger
from avro import schema

path = os.path.dirname(os.path.realpath(__file__))
path_configs = os.path.join(path, 'configs')

_schema = json.dumps({"type": "record", "name": "X", "fields": [{"name": "y", "type": {"type": "record", "name": "Y", "fields": [{"name": "Z", "type": "X"}]}}]})
version = 2
name = 'test'


class TestRegistry(unittest.TestCase):

    def test_get_schema(self):
        """
        Return schema name and version specific and the compare with avro.Parse.
        """
        logger.setLevel(logging.DEBUG)
        logger.info("testing")

        register = Registry(path_configs)
        r_schema = register.get(name, version)

        if six.PY3:
            original_parse = schema.Parse(_schema)
        else:
            original_parse = schema.parse(_schema)

        self.assertEqual(r_schema, original_parse)

    def test_fail_schema_version(self):
        """
           To test exception raise due to run time error
        """
        logger.setLevel(logging.DEBUG)
        logger.info("testing")
        # not found version 2
        version = 1
        self.register = Registry(path_configs)
        self.assertRaises(SchemaVersionNotFound, lambda: self.register.get(name, version))

    def test_fail_schema_not_exist(self):
        """
           To test exception raise due to run time error
        """

        logger.setLevel(logging.DEBUG)
        logger.info("testing")
        version = 1
        name = 'test_1'

        self.register = Registry(path_configs)
        self.assertRaises(SchemaNotFound, lambda: self.register.get(name, version))

    def test_path_schemas(self):
        """
        """
        logger.info("Schemas for test.>  %s" % path_configs)
        if not os.path.exists(path_configs):
            logger.info("Add Directory for schemas.")
        assert os.path.exists(path_configs)


if __name__ == '__main__':
    unittest.main()
