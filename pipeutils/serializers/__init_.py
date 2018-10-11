import os
import io
import six
import avro.io
import json
import functools

from avro import schema
from avro.schema import AvroException
from avro.io import AvroTypeException

from collections import OrderedDict


if six.PY2:
    from avro.io import validate
else:
    from avro.io import Validate as validate


class SerializerError(Exception):

    """Generic error from serializer package"""

    def __init__(self, message):
        self.message = message

        def __repr__(self):
            return 'SerializerError(error={error})'.format(error=self.message)

        def __str__(self):
            return self.message


class AvroJsonBase(object):
        
    ''' Apache Avro schemas consist of two kinds of types:
         * Primitive Type
         * Complex Type
    '''

    PRIMITIVE_TYPE = frozenset([
        "int",
        "long",
        "float",
        "double",
        "string",
        "bytes",
        "boolean",
        "null",
    ])

    """Internally used to distinguish from "null" values on dict.get."""
    class UnsetValue(object):
        pass
    UNSET = UnsetValue()

    def __init__(self):
        self.COMPLEX_TYPE_SERIALIZER = {
            "record": self._serialize_record,
            "array": self._array,
            "map": self._map,
            "union": self._serialize_union,
            "error_union": self._serialize_union,
            "request":self._serialize_record,
            "error": self._serialize_record,
            "fixed":self._serialize_bytes,
            "bytes": self._serialize_bytes
        }

        self.COMPLEX_TYPE_DESERIALIZER = {
            "array": self._array,
            "map": self._map,
            "union": self._deserialize_union,
            "error_union": self._deserialize_union,
            "record": self._deserialize_record,
            "request": self._deserialize_record,
            "error": self._deserialize_record,
            "fixed": self._desserialize_bytes,
            "bytes": self._desserialize_bytes
        }

    def _validate(self, schema, data):
        return validate(schema, data)

    def _union_name(self, schema):
        name = schema.type
        if isinstance(schema, avro.schema.NamedSchema):
            if schema.namespace:
                name = schema.fullname
            else:
                name = schema.name
        return name
    def _array(self, schema, data):
        if data is None:
            raise AvroTypeException(schema, data)
        process = functools.partial(self._process_serializer, schema.items)
        return list(map(process, data))
    
    def _map(self, schema, data):
        if data is None:
            raise AvroTypeException(schema, data)
        process = functools.partial(self._process_serializer, schema.values)
        return dict((key, process(value)) for key, value in six.iteritems(data))

    def _serialize_record(self, schema, data):
        result = OrderedDict()
        for field in schema.fields:
            result[field.name] = self._process_serializer(field.type,
                                                          data.get(field.name))
        return result

    def _serialize_bytes(self, schema, data):
        return data.decode("ISO-8859-1")

    def _serialize_union(self, schema, data):
        for candidate_schema in schema.schemas:
            if validate(candidate_schema, data):
                if candidate_schema.type == "null":
                    return None 
                else:
                    field_type_name = self._union_name(candidate_schema)
                    return {
                        field_type_name: self._process_serializer(candidate_schema, data)
                    }
        raise AvroTypeException(schema, data)

    def _process_serializer(self, schema, data):
        if not self._validate(schema, data):
            raise AvroTypeException(schema, data)

        if schema.type in self.PRIMITIVE_TYPE:
            return data

        if schema.type in self.COMPLEX_TYPE_SERIALIZER:
            return self.COMPLEX_TYPE_SERIALIZER[schema.type](schema, data)

        raise avro.schema.AvroException("Unknown type: %s" % schema.type)

    def _deserialize_union(self, schema, data):
        for _schema in schema.schemas:
            if self._validate_union(_schema, data):
                if _schema.type == "null":
                    return None
                else:
                    field_type_name = self._union_name(_schema)
                    return self._process_deserializer(_schema,
                                                       data[field_type_name])
        raise AvroTypeException(schema, data)

    def _deserialize_record(self, schema, data):
        result = OrderedDict()
        for field in schema.fields:
            result[field.name] = self._process_deserializer(field.type,
                                                             data.get(field.name, self.UNSET))
        return result

    def _desserialize_bytes(self, schema, data):
        if isinstance(data, bytes):
            return data

        return data.encode("ISO-8859-1")

    def _process_deserializer(self, schema, data):
        if not self._validate(schema, data):
            raise AvroTypeException(schema, data)

        if schema.type in self.PRIMITIVE_TYPE:
            return data

        if schema.type in self.COMPLEX_TYPE_DESERIALIZER:
            return self.COMPLEX_TYPE_DESERIALIZER[schema.type](schema, data)

