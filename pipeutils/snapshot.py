import os
import requests
import gzip

from datetime import date
from pyquery import PyQuery as pq
from pipeutils import logger

def create(url, params={}, prefix='', sufix='', output=None):

    """Gets the content of a page, store a html.gz and returns a document to process.
    Args:
        url (str): The url to get the page.
        params (dict): The params for the url request
        prefix (str): The prefix for the beginning of the name of each page.
        sufix (str): The suffix for the end of the name of each page and differentiated with the number.
        output (str): The output directory to store the resulted file html.gz
    Returns:
        The return content of a page
    """

    logger.debug(' > URL: %s ', url)
    try:
        response = requests.get(url, params=params)
    except:
        logger.debug("Connection refused by the server..")
        logger.debug("Was a nice sleep, now let me continue...")

    if output is not None:
        today = date.today().strftime('%Y-%m-%d')
        path = os.path.join(output, prefix + today + str(sufix) + '.html.gz')
        with gzip.open(path, 'wb') as f:
            f.write(response.content)
        logger.debug(' > Stored at: %s ', path)

    return response.content


def read(file, compress=None):
    """Gets the content of a page saved as html.gz, for procces the content and returns ```document```.
    Args:
        file (str): The file parameter for process.
        compress (str):  The compress parameter to open the resulted file html.gz
    Returns:
        The return content of a page.
    """

    if compress is None:
        print('into')
        with open(_file, 'rb') as f:
            doc = pq(f.read())
        return doc

    if compress:
        with gzip.open(file, 'rb') as f:
            doc = pq(f.read())
        return doc
