class ConfigNotFound(ValueError):
    pass


class SerializerError(Exception):
    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return 'SerializerError(error={error})'.format(error=self.message)

    def __str__(self):
        return self.message
