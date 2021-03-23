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

_avro_test_1 =json.dumps({"type" : "record", "namespace" : "test", "name" : "Numero_1", "fields" : [{ "name" : "Name" , "type" : "string" }, { "name" : "Age" , "type" : "int" }]})

_avro_test_2 =json.dumps({"type" : "record", "namespace" : "test", "name" : "Numero_2", "fields" : [{ "name" : "Ocupation" , "type" : "string" }, { "name" : "Direction" , "type" : "string" }]})


_avro_test_3 =json.dumps({"type" : "record", "namespace" : "test", "name" : "Numero_3", "fields" : [{ "name" : "Email" , "type" : "string" }, { "name" : "Number" , "type" : "int" }]})


_avro_test_4 =json.dumps({"type" : "record", "namespace" : "test", "name" : "Numero_4", "fields" : [{ "name" : "NickName" , "type" : "string" }]})


_avro_test_5 =json.dumps({"type" : "record", "namespace" : "test", "name" : "Numero_5", "fields" : [{ "name" : "Other" , "type" : "string" }, { "name" : "X" , "type" : "int" }]})



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
        version = 2
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
    
    def test_schema_versions(self):
        """
        """
        logger.info("Schemas")
        name = 'property'
        version = 1
        self.register = Registry(path_configs)

        r_schema_1 = self.register.get(name, version=1)
        r_schema_2 = self.register.get(name, version=2)
        r_schema_3 = self.register.get(name, version=3)
        r_schema_4 = self.register.get(name, version=4)
        r_schema_5 = self.register.get(name, version=5)

        if six.PY3:
            _file_test_1 = schema.Parse(_avro_test_1)
            _file_test_2 = schema.Parse(_avro_test_2)
            _file_test_3 = schema.Parse(_avro_test_3)
            _file_test_4 = schema.Parse(_avro_test_4)
            _file_test_5 = schema.Parse(_avro_test_5)
        else:
            _file_test_1 = schema.parse(_avro_test_1)
            _file_test_2 = schema.parse(_avro_test_2)
            _file_test_3 = schema.parse(_avro_test_3)
            _file_test_4 = schema.parse(_avro_test_4)
            _file_test_5 = schema.parse(_avro_test_5)

        self.assertEqual(r_schema_1, _file_test_1)
        self.assertEqual(r_schema_2, _file_test_2)
        self.assertEqual(r_schema_3, _file_test_3)
        self.assertEqual(r_schema_4, _file_test_4)
        self.assertEqual(r_schema_5, _file_test_5)

        self.assertRaises(SchemaVersionNotFound, lambda: self.register.get(name, version=6))
     

if __name__ == '__main__':
    unittest.main()
