import os
import requests
import gzip
import pathlib

from datetime import date
from pyquery import PyQuery as pq
from pipeutils import logger

def create(url, params={}, headers={}, prefix='', sufix='', output=None):

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
        response = requests.get(url, params=params, headers=headers)
    except:
        logger.debug("Connection refused by the server..")
        logger.debug("Was a nice sleep, now let me continue...")

    if output is not None:
        if not os.path.exists(output):
            os.makedirs(output)
        today = date.today().strftime('%Y-%m-%d')
        path = os.path.join(output, prefix + today + str(sufix) + '.html.gz')
        with gzip.open(path, 'wb') as f:
            f.write(response.content)
        logger.debug(' > Stored at: %s ', path)

    return response.content


def read(filepath):
    """Gets the content of a page saved as html.gz, for procces the content and returns ```document```.
    Args:
        filepath (str): The file path parameter is the path of the file to be read.
    Returns:
        The return content of a page.
    """
    if '.gz' or '.zip' in pathlib.Path(filepath).suffixes:
        with gzip.open(filepath, 'rb') as f:
            doc = pq(f.read())
        return doc
    with open(filepath, 'rb') as f:
        doc = pq(f.read())
    return doc
