import unittest
import os
import logging
import random
import six
import avro

from pipeutils.serializers.serializer import AvroSerializer, JSONSerializer
from pipeutils import logger

if six.PY2:
    from avro.schema import make_avsc_object
else:
    from avro.schema import SchemaFromJSONData as make_avsc_object

path = os.path.dirname(os.path.realpath(__file__))
path_configs = os.path.join(path, 'configs')

version = 3
name = 'test'


class Testserialize(unittest.TestCase):

    def test_get_serialize_avro(self):
        """
        """
        logger.setLevel(logging.DEBUG)
        logger.info("testing")

        serializer = AvroSerializer('test', version, 'codetest')
        data = {"name": "TEXT INTO MESSAGE", "favorite_color": "111", "favorite_number": random.randint(0, 10)}
        kwargs = {
            'schema': 'test',
            #'version': 3
        }
        serialize = serializer.serialize(data, version, pipeline=None, **kwargs)
        self.assertIn(bytes("TEXT INTO MESSAGE", "utf-8"), serialize)

    def test_get_deserialize_avro(self):
        """
        """
        serializer = AvroSerializer('test', version, 'codetest')

        data = {"name": "test", "favorite_color": "Red", "favorite_number": 0}
        kwargs = {
            'schema': 'test',
            #'version': 3
        }

        bytes_text = serializer.serialize(data, version, pipeline=None, **kwargs)
        serialize = serializer.deserialize(bytes_text, version, pipeline=None, **kwargs)
        self.assertEqual(serialize, data)

    def test_get_serialize_json(self):
        """
        """
        logger.setLevel(logging.DEBUG)
        logger.info("testing")

        serializer = JSONSerializer()
        data = {"name": "TEXT INTO MESSAGE", "favorite_color": "111", "favorite_number": 2}

        schema_dict = {
              "type": "record",
              "name": "User",
              "fields": [
                  {"name": "name", "type": "string"},
                  {"name": "favorite_number",  "type": ["int", "null"]},
                  {"name": "favorite_color", "type": ["string", "null"]}
              ]
        }
        avro_schema = make_avsc_object(schema_dict, avro.schema.Names())

        kwargs = {
            'schema': avro_schema
        }

        serialize = serializer.serialize(data, **kwargs)
        logger.info("DATA %s" % serialize)

        args = {
            'schema': 'test',
            'version': 3,
        }

        # GET SCHEMA.
        avro_schema = serializer.get_schema(args)

        kwargs = {
            'schema': avro_schema
        }

        json_data = serializer.serialize(data, **kwargs)
        _avro = '{"name":"TEXT INTO MESSAGE","favorite_color":"111","favorite_number":2}'
        
        logger.info("DATA %s" % _avro)

        self.assertEqual(json_data, _avro)

    def test_get_deserialize_json(self):
        """
        """
        logger.setLevel(logging.DEBUG)
        logger.info("testing")

        serializer = JSONSerializer()
        data = {"name":"TEXT INTO MESSAGE","favorite_color":"111","favorite_number":2}

        args = {
            'schema': 'test',
            'version': 3,
        }

        # GET SCHEMA.
        avro_schema = serializer.get_schema(args)

        kwargs = {
            'schema': avro_schema
        }
        data = serializer.serialize(data, **kwargs)

        dump_data = serializer.deserialize(data, **kwargs)
        logger.info('deserialize %s' % dump_data)
        self.assertEqual(dump_data, data)

if __name__ == '__main__':
    unittest.main()

