import hashlib
import six
import uuid


def generate_id(*args):
    """
    Generate a code based in md5, receive a tuple of strings and return a
    number string of 32 characters.
    """
    concated = ''.join([six.text_type(x) for x in args])
    m = hashlib.md5()
    m.update(concated.encode('utf-8'))
    return six.text_type(m.hexdigest())


def generate_uuid(phrase, size=None):
    """
    Generate a code base in the DNS namcespace, receive the params phrase that
    is a string and a size that receive a integer, the function return  a
    string with the uuid code generated.
    """
    res = str(uuid.uuid3(uuid.NAMESPACE_DNS, phrase))
    if size is None:
        return res
    else:
        return res[:size]
