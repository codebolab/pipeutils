import os
import json

from collections import OrderedDict

HOME = os.path.expanduser("~")
VERSION = 1



class Registry:

    '''
        A schema registry should manage the avro schemas and versions.
    '''
    def __init__(self, _path=None):
        self.schema = {}
        if _path is not None:
            self.path = _path
        elif os.environ.get('PIPE_SCHEMA_REGISTRY'):
            self.path = os.environ.get('PIPE_SCHEMA_REGISTRY')
        else:
            self.path = os.path.join(HOME, '.pipeutils', 'registry')

    def _cache_schema(self, entry, key):
        # overwrite, not much performance impact, as shouldn't be happening often
        self.schema[key] = entry

    def register(self, name):
        for f in os.listdir(self.path):
            if f.endswith('.json') and os.path.splitext(f)[0] == name:
                with open(os.path.join(self.path, f), 'r') as _file:
                    json_data = _file.read()
                    data = json.loads(json_data)
                    key_base = f.replace('.json', '')
                for entry in data:
                    key = "%s_%s" % (key_base, entry.get('name'))
                    self._cache_schema(entry, key)
        return self.schema

    def get(self, name=None, version=VERSION):
        result = OrderedDict()
        _schema = self.register(name)
        if _schema:
            for datum in _schema.values():
                for v in datum['schema']:
                    if float(v) == version:
                        path = os.path.join(self.path, datum['schema'][v])
                        with open(path) as f:
                            result['schema'] = f.read()
                            result['name'] = name
                            result['version'] = version
        else:
            raise ValueError('invalid schama not fount: %s' % name)
        return result