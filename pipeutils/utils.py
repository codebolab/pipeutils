import hashlib
import six


def generate_id(*args):
    """
    Generate a code based in md5, receive a tuple of strings and return a 
    number string of 32 characters.
    """
    concated = ''.join([six.text_type(x) for x in args])
    m = hashlib.md5()
    m.update(concated.encode('utf-8'))
    return str(m.hexdigest())
