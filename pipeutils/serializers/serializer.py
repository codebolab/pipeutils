import os
import io
import six
import avro.io
import json

from avro import schema
from pipeutils import logger
from pipeutils.avro import Registry, registry


class Serializer(object):
    pass


class AvroSerializer(Serializer):

    def get_schema(self, schema_name, version=1):
        if schema_name:
            self._schema_name = schema_name
        if version:
            self._version = version

        key = '%s_%s' % (self._schema_name, self._version)
        if key in registry.cache_schemas:
            logger.debug('key : %s ', key)
            return registry.cache_schemas[key]
        else:
            return registry.get(self._schema_name, self._version)

    def serialize(self, data, pipeline=None, version=1, **kwargs):
        """
        Returns the avro encoded version of `data` using the avro schema `pipeline` and its `version`.
        """
        raw_bytes = None
        schema =  self.get_schema(schema_name=pipeline, version=version)
        try:
            writer = avro.io.DatumWriter(schema)
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write(data, encoder)
            raw_bytes = bytes_writer.getvalue()
            logger.debug('Read bytes: %s ', len(raw_bytes))
            logger.debug(type(raw_bytes))
        except:
            logger.debug('Error serializer data: %s ', data)

        return raw_bytes

    def deserialize(self, data, pipeline=None, version=1, **kwargs):
        """
        Returns the avro decoded version of `data` using the avro schema `pipeline` and its `version`.
        """
        bytes_reader = io.BytesIO(data)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        schema =  self.get_schema(schema_name=pipeline, version=version)
        reader = avro.io.DatumReader(schema)
        datum = reader.read(decoder)
        logger.debug('Data : %s ', datum)
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

