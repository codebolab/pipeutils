import os
import io
import six
import avro.io
import json

from avro import schema
from pipeutils import logger
from pipeutils.avro import Registry, registry


class Serializer(object):

    def __init__(self, schema_name, version):
        self._schema_name = schema_name
        self._version = version

    def get_schema(self, schema_name, version=1):
        if schema_name:
            self._schema_name = schema_name
        if version:
            self._version = version

        _schema = registry.get(self._schema_name, self._version)
        return _schema

    def _check_schemas(self, _type, _fschema):
        schema = _fschema()
        key = '%s_%s' % (self._schema_name, self._version)

        if key in registry.cache_schemas:
            if _type == 'W':
                return avro.io.DatumWriter(registry.cache_schemas[key])
            if _type == 'R':
                return avro.io.DatumReader(registry.cache_schemas[key])
        else:
            if _type == 'W':        
                return avro.io.DatumWriter(schema)
            if _type == 'R':
                return avro.io.DatumReader(schema)


class AvroSerializer(Serializer):

    def serialize(self, data, pipeline=None, version=1, **kwargs):
        """
        Returns the avro encoded version of `data` using the avro schema `pipeline` and its `version`.
        """
        writer = self._check_schemas('W',_fschema=lambda: self.get_schema(schema_name=pipeline, version=version))
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(data, encoder)
        data = bytes(bytearray(bytes(self._version)))
        data += bytes_writer.getvalue()
        logger.debug('Read bytes: %s ', len(data))
        return data

    def deserialize(self, data, pipeline=None, version=1, **kwargs):
        """
        Returns the avro decoded version of `data` using the avro schema `pipeline` and its `version`.
        """
        bytes_reader = io.BytesIO(data)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader =  self._check_schemas('R',_fschema=lambda: self.get_schema(schema_name=pipeline, version=version))
        datum = reader.read(decoder)
        logger.debug('Data : %s ', datum)
        return reader.read(decoder)


class JSONSerializer(Serializer):

    def serialize(self, data, **kwargs):
        """
        Returns the json encoded version of `data`.
        """
        return json.dumps(data)#, separators=(",", ":"))

    def deserialize(self, data, **kwargs):
        """
        Returns the json decoded version of `data`.
        """
        return json.loads(data)

