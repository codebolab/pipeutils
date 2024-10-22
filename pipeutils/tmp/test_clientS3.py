import unittest
import os

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
        bucket = self.client.clientS3.Bucket(self.client.bucket)
        objs = bucket.objects.all()
        objs.delete()

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
            passed = False
            logger.info(f"No such config file in {str(e)}")

        assert passed

    def test_upload(self):
        files_path = os.path.join(PATH, 'files', 'file1.txt')
        logger.info('Files path %s' % files_path)
        try:
            self.client.upload(files_path, 'file1.txt')
            passed = True
        except Exception as e:
            logger.info(f"No such config file in {str(e)}")
            passed = False
        assert passed

    def test_download(self):
        path_down = os.path.join(PATH, 'files', 'file1-down.txt')
        passed = False
        self.client.download('file1.txt', path_down)
        try:
            passed = True
        except Exception as e:
            passed = False
            logger.info(f"No such config file' in {str(e)}")

        assert passed

    def test_upload_multiple(self):
        directory_path = os.path.join(PATH, 'files', 'multiple')
        s3_directory_path = os.path.join('test', 'multiple')
        logger.info('Files path %s' % directory_path)
        list_files = []
        try:
            self.client.upload_multiple(directory_path, s3_directory_path,
                                        extension='txt')
            bucket = self.client.clientS3.Bucket(self.client.bucket)
            list_test = bucket.objects.all()
            list_files = list(map(lambda x: x._key, list(list_test)))
            passed = True
        except Exception as e:
            logger.error(e)
            passed = False
        assert passed
        assert 'test/multiple/file1.txt' in list_files
        assert 'test/multiple/file2.txt' in list_files

    def test_upload_recursive_directories(self):
        '''
        Test the files uploaded recursively in s3
        '''
        directory_path = os.path.join(PATH, 'files', 'recursive')
        s3_directory_path = os.path.join('test', 'recursive')
        list_files = []

        try:
            self.client.upload_recursive(directory_path, s3_directory_path,
                                         extension='json')
            bucket = self.client.clientS3.Bucket(self.client.bucket)
            list_test = bucket.objects.all()
            list_files = list(map(lambda x: x._key, list(list_test)))
            passed = True
        except Exception as e:
            logger.error(e)
            passed = False

        self.client.upload_recursive(directory_path, s3_directory_path,
                                     extension='json')
        assert passed
        assert 'test/recursive/file.json' in list_files
        assert 'test/recursive/file.txt' not in list_files
        assert 'test/recursive/directory1/file.json' in list_files
        assert 'test/recursive/directory2/file1.json' in list_files
        assert 'test/recursive/directory2/file2.json' in list_files
        assert 'test/recursive/directory2/file3.txt' not in list_files
        assert 'test/recursive/directory1/directory/file.json' in list_files


if __name__ == '__main__':
    unittest.main()
