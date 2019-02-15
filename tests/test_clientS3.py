import unittest
import os
import logging

from pipeutils import config
from pipeutils.clients.client_s3 import ClientS3
from pipeutils import logger

PATH = os.path.dirname(os.path.realpath(__file__))


class TestCientS3(unittest.TestCase):
    def setUp(self):
        self.client = ClientS3('test-pipeutils')

    def tearDown(self):
        '''connections and will close automatically after a few minutes of idle time.
        I wouldn't worry about trying to close them.
        '''
        #self.client.close()
        pass

    def test_list(self):
        """
        Find list for success.
        """
        passed = False
        try:
            list_s3 = self.client.list()
            logger.info('list s3 > %s' % list_s3)
            passed = True
        except Exception as e:
            passed = 'No such config file' in str(e)

    def test_upload(self):
        files_path = os.path.join(PATH, 'files', 'file1.txt')
        logger.info('Files path %s' % files_path)
        try:
            self.client.upload(files_path, 'file1.txt')
            passed = True
        except Exception as e:
            passed = 'No such config file' in str(e)

    def test_download(self):
        path_down = os.path.join(PATH, 'files', 'file1-down.txt')
        passed = False
        self.client.download('/test-pipeutils/file1.txt', path_down)
        try:
            passed = True
        except Exception as e:
            passed = 'No such config file' in str(e)

if __name__ == '__main__':
    unittest.main()
