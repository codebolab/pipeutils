import unittest
import os
import logging
import six
import json

from pipeutils.avro import Registry
from pipeutils import logger
from avro import schema

path = os.path.dirname(os.path.realpath(__file__))
path_configs = os.path.join(path, 'configs')

_schema = json.dumps({"type": "record", "name": "X", "fields": [{"name": "y", "type": {"type": "record", "name": "Y", "fields": [{"name": "Z", "type": "X"}]}}]})
version = 1.0
name = 'test'

class TestRegistry(unittest.TestCase):

    def test_get_schema(self):
        """
        """
        logger.setLevel(logging.DEBUG)
        logger.info("testing")

        register = Registry(path_configs)
        test = register.get(name, version)
        self.assertEqual(test['version'], version)

        logger.info(schema.Parse(test['schema']))
        if six.PY3:
            original_parse = schema.Parse(_schema)
            result_parse = schema.Parse(test['schema'])
        else:
            original_parse = schema.parse(_schema)
            result_parse = schema.parse(test['schema'])

        self.assertEqual(result_parse, original_parse)


if __name__ == '__main__':
    unittest.main()