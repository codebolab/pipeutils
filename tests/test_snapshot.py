import unittest
import os
import logging
import gzip
import requests
import json

from pipeutils import logger
from datetime import date

from pipeutils.snapshot import create, read

script_path = os.path.dirname(os.path.abspath( __file__ ))
data_path = os.path.join(script_path, 'data')
file_path = os.path.join(data_path, 'file.html.gz')


class TesSnapShot(unittest.TestCase):
    '''
    def test_create(self):
        """
         Execute the ```create``` function and verify if the file was created.
        """
        logger.setLevel(logging.DEBUG)
        logger.info("testing")
        url = 'https://www.facebook.com/elvikito'
        params = {}
        
        _file = create(url, params)
        self.assertIsNotNone(_file)

        response = requests.get(url, params=params)
        content = response.content
        self.assertEqual(_file[1], content[1])

    def test_read(self):
        """
          Execute the ```read``` function and verify if the file exist.
        """
        logger.info("Read")
        try:
            _file = read(file_path, compress=True)
            print(_file)
            self.assertIsNotNone(_file)
            passed = True
        except Exception as e:
            passed = 'No such file' in str(e)
        self.assertEqual(passed, True)
    '''

    def test_header(self):
        """
          Execute the ```create``` function and verify if header change.
        """
        url = 'http://worldagnetwork.com/'
        params = {}
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        _file = create(url, params, headers)
        self.assertIsNotNone(_file)
        response = requests.get(url, params=params)
        response = requests.get(url, headers=headers)
        content = response.content
        self.assertEqual(_file[1], content[1])


if __name__ == '__main__':
    unittest.main()
