import unittest
import logging
import random
from pipeutils.serializers import AvroSerializer, JSONSerializer
from pipeutils import logger


logger.setLevel(logging.DEBUG)


VERSION = 3
NAME = 'test'


class Testserialize(unittest.TestCase):

    def test_serialize_avro(self):
        logger.info("testing")

        serializer = AvroSerializer(NAME, version=VERSION)
        data = {
            "name": "TEXT INTO MESSAGE",
            "favorite_color": "111",
            "favorite_number": random.randint(0, 10)
        }
        serialize = serializer.serialize(data)
        self.assertIn(bytes("TEXT INTO MESSAGE", "utf-8"), serialize)

    def test_deserialize_avro(self):
        serializer = AvroSerializer(NAME, version=VERSION)
        data = {
            "name": "test",
            "favorite_color": "Red",
            "favorite_number": 0
        }
        bytes_text = serializer.serialize(data)
        serialize = serializer.deserialize(bytes_text)
        self.assertEqual(serialize, data)

    def test_serialize_json(self):
        serializer = JSONSerializer()

        data = {
            "name": "TEXT INTO MESSAGE",
            "favorite_color": "Black",
            "favorite_number": 2
        }

        json_serialized = serializer.serialize(data)
        serialized = (
            '{"name": "TEXT INTO MESSAGE", '
            '"favorite_color": "Black", "favorite_number": 2}'
        )

        self.assertEqual(json_serialized, serialized)

    def test_get_deserialize_json(self):
        """
        """
        serializer = JSONSerializer()

        data = {
            "name": "TEXT INTO MESSAGE",
            "favorite_color": "Black",
            "favorite_number": 2
        }

        json_serialized = serializer.serialize(data)
        serialized = (
            '{"name": "TEXT INTO MESSAGE", '
            '"favorite_color": "Black", "favorite_number": 2}'
        )
        self.assertEqual(json_serialized, serialized)

        deserialized = serializer.deserialize(json_serialized)
        self.assertEqual(data, deserialized)


if __name__ == '__main__':
    unittest.main()
