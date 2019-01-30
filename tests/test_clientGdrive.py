import unittest
import os
import logging
import json
import gdrive_client

from pipeutils import config
from pipeutils.clients_GDrive import GDrive as DriveClient
from pipeutils import logger
PATH = os.path.dirname(os.path.realpath(__file__))


class TestCientGdrive(unittest.TestCase):
    def get_client(self):
        client = DriveClient.GDrive()
        return client

    def test_get_list(self):
        client = self.get_client()
        files = client.listfiles()
        assert len(files) > 0
        assert files is not None
        logger.info('Files %s' % files)

    def test_download(self):
        client = self.get_client()
        path = os.path.join(os.getcwd(), 'files')
        client.download('file1.txt', path)
        assert os.path.exists(path+'/file1.txt') is True

    def test_upload(self):
        client = self.get_client()
        cwd_dir = os.path.join(os.getcwd(), 'files')
        file_upload = os.path.join(cwd_dir, "file1.txt")
        upload = client.upload(file_upload, 'test')
        assert upload is not None
  
if __name__ == '__main__':
    unittest.main()

