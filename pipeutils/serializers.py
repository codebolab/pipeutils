import io
import avro.io
import json

from pipeutils import logger
from pipeutils.avro import registry


class Serializer(object):
    def serialize(self, data, **kwargs):
        raise NotImplementedError

    def deserialize(self, data, **kwargs):
        raise NotImplementedError


class AvroSerializer(Serializer):
    def __init__(self, schema_name, version=1):
        self.schema = registry.get(name=schema_name, version=version)

    def serialize(self, data):
        """
        Returns the avro encoded version of `data` using the avro schema
        `pipeline` and its `version`.
        """
        raw_bytes = None

        try:
            writer = avro.io.DatumWriter(self.schema)
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write(data, encoder)
            raw_bytes = bytes_writer.getvalue()
        except Exception as e:
            logger.error(f'{e} serializer data: %s ')

        return raw_bytes

    def deserialize(self, data):
        """
        Returns the avro decoded version of `data` using the avro schema
        `pipeline` and its `version`.
        """

        bytes_reader = io.BytesIO(data)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(self.schema)
        datum = reader.read(decoder)
        return datum


class JSONSerializer(Serializer):
    def serialize(self, data, **kwargs):
        """
        Returns the json encoded version of `data`.
        """
        return json.dumps(data)

    def deserialize(self, data, **kwargs):
        """
        Returns the json decoded version of `data`.
        """
        return json.loads(data)
