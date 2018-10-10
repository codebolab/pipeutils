import os
import io
import six
import avro.io
import json

from avro import schema
from avro.schema import AvroException
from avro.io import AvroTypeException
from pipeutils.serializers import SerializerError, AvroJsonBase
from pipeutils.avro import Registry


class Serializer(object):

    def __init__(self, schema, version, topic):
        self._schema = schema
        self._version = version
        self._topic = topic
        self._schemas = {}


    def get_schema(self, args):
        if 'schema' in args:
            self._schema = args['schema']
        if 'version' in args:
            self._version = args['version']
        if 'topic' in args:    
            self._topic = args['topic']

        _schema = None
        registry = Registry()
        try:
            _schema, found = registry.get(self._schema, self._version)
        except:
            _schema = None

        if not _schema:
            err = "unable to fetch schema %s with version %s" % (self._schema, self._version)
            raise SerializerError(err)

        return _schema

    def _cache(self):
        return '%s_%s' % (self._topic, self._version)

    def _check_schemas(self, _fschema):
        schema = _fschema()
        schema_id = self._cache()
        if schema_id in self._schemas:
            return self._schemas[schema_id]

        if schema:
            self._schemas[schema_id] = avro.io.DatumWriter(schema)
        else:
            self._schemas[schema_id] = None
        return self._schemas[schema_id]


class AvroSerializer(Serializer):

    def serialize(self, data, version, pipeline=None, **kwargs):
        """
        Returns the avro encoded version of `data` using the avro schema `pipeline` and its `version`.
        """
        kwargs = dict(kwargs, version=version)
        writer = self._check_schemas(_fschema=lambda: self.get_schema(kwargs))
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(data, encoder)
        data = bytes(bytearray(bytes(self._version)))
        data += bytes_writer.getvalue()
        print("Read {} bytes".format(len(data)))
        return data

    def deserialize(self, data, version, pipeline=None, **kwargs):
        """
        Returns the avro decoded version of `data` using the avro schema `pipeline` and its `version`.
        """
        kwargs = dict(kwargs, version=version)
        bytes_reader = io.BytesIO(data)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(self.get_schema(kwargs))
        datum = reader.read(decoder)
        print(" Data = {}".format(datum))
        return reader.read(decoder)


class JSONSerializer(AvroJsonBase, Serializer):

    def __init__(self):
        super(JSONSerializer, self).__init__()

    def hash_func(self):
        return hash(str(self))

    schema.EnumSchema.__hash__ = hash_func
    schema.RecordSchema.__hash__ = hash_func
    schema.PrimitiveSchema.__hash__ = hash_func
    schema.ArraySchema.__hash__ = hash_func
    schema.FixedSchema.__hash__ = hash_func
    schema.MapSchema.__hash__ = hash_func
    
    def serialize(self, data, **kwargs):
        """
        Returns the json encoded version of `data`.
        """
        if 'schema' in kwargs:
            self.schema = kwargs['schema']
            
        result = self._process_serializer(self.schema, data)
        # we use separators to minimize size of the output string
        return json.dumps(result, separators=(",", ":"))

    def deserialize(self, data, **kwargs):
        """
        Returns the json decoded version of `data`.
        """

        if 'schema' in kwargs:
            self.schema = kwargs['schema']
        
        _dict = self._process_deserializer(self.schema, json.loads(data))
        return _dict
