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


def generate_uuid(phrase, size=8):
    """
    Generate a code base in the DNS namcespace, receive the params phrase that
    is a string and a size that by default is integer 8 and return  a string
    with the uuid code generated.
    """
    return str(uuid.uuid3(uuid.NAMESPACE_DNS, phrase))[:size]
