import hashlib


def generate_id(,*args):
    """
    Generate a code based in md5, receive a tuple of strings and return a 
    number string of 32 characters.
    """
    m = hashlib.md5()
    m.update(''.join(args).encode('utf-8'))
    return str(int(m.hexdigest(), 16))[0:32]
