import unittest
import os
import random
import six
import avro
import json

from pipeutils.serializers.serializer import AvroSerializer, JSONSerializer
from pipeutils.avro import Registry
from pipeutils import logger

if six.PY2:
    from avro.schema import make_avsc_object
else:
    from avro.schema import SchemaFromJSONData as make_avsc_object

path = os.path.dirname(os.path.realpath(__file__))
path_configs = os.path.join(path, 'configs')

version = 3
name = 'test'

registry = Registry(path_configs)
os.environ['PIPE_SCHEMA_REGISTRY'] = '{0}'.format(path_configs)


class Testserialize(unittest.TestCase):

    def test_get_serialize_avro(self):
        """
        """
        serializer = AvroSerializer()
        data = {"name": "TEXT INTO MESSAGE", "favorite_color": "111",
                "favorite_number": random.randint(0, 10)}
        logger.info("export PIPE_SCHEMA_REGISTRY={}".format(path_configs))
        if os.environ.get('PIPE_SCHEMA_REGISTRY'):
            serialize = serializer.serialize(data, name, version)
            self.assertIn(bytes("TEXT INTO MESSAGE", "utf-8"), serialize)

    def test_get_deserialize_avro(self):
        """
        """
        serializer = AvroSerializer()

        data = {"name": "test", "favorite_color": "Red", "favorite_number": 0}

        logger.info("export PIPE_SCHEMA_REGISTRY with {}".format(path_configs))
        if os.environ.get('PIPE_SCHEMA_REGISTRY'):
            bytes_text = serializer.serialize(data, name, version)
            serialize = serializer.deserialize(bytes_text, name, version)
            self.assertEqual(serialize, data)

    def test_get_serialize_json(self):
        """
        """
        serializer = JSONSerializer()

        data = {
            "nama": "TEXT INTO MESSAGE",
            "favorite_color": "Black",
            "favorite_number": 2
        }

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
            'name': 'test',
            'version': 3,
        }

        # GET SCHEMA.
        avro_schema = registry.get(**args)

        kwargs = {
            'schema': avro_schema
        }

        json_data = serializer.serialize(data, **kwargs)

        _avro = {
            "nama": "TEXT INTO MESSAGE",
            "favorite_color": "Black",
            "favorite_number": 2
        }

        self.assertEqual(json_data, json.dumps(_avro))

    def test_get_deserialize_json(self):
        """
        """
        serializer = JSONSerializer()

        data = {
            'name': 'TEXT INTO MESSAGE',
            'favorite_color': 'RED',
            'favorite_number': 2
        }

        args = {
            'name': 'test',
            'version': 3,
        }

        # GET SCHEMA.
        avro_schema = registry.get(**args)

        kwargs = {
            'schema': avro_schema
        }
        _data = serializer.serialize(data, **kwargs)

        dump_data = serializer.deserialize(_data, **kwargs)
        self.assertEqual(data, dump_data)


if __name__ == '__main__':
    unittest.main()
