import unittest
import os

from pipeutils.clients.client_GDrive import GDrive
from pipeutils import logger


class TestCientGdrive(unittest.TestCase):
    def get_client(self):
        client = GDrive()
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
