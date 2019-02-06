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
            _file = read(file_path, compress='gzip')
            self.assertIsNotNone(_file)
            passed = True
        except Exception as e:
            passed = 'No such file' in str(e)
        self.assertEqual(passed, True)

if __name__ == '__main__':
    unittest.main()
