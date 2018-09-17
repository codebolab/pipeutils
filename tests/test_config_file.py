import unittest
import os
import logging

CONFIG_PATH = os.environ.get('PIPE_CONFIG_PATH')
path = os.path.dirname(os.path.realpath(__file__))
print("path: %s " % path)
from pipeutils import config
from pipeutils import logger


class TestConfigFile(unittest.TestCase):

    def test_config(self):
        """
        Check that attempting to open a non-existent config file raises
        an Exception with appropriate message.
        """
        passed = False
        logger.setLevel(logging.DEBUG)
        logger.info("testing")
        try:
            c = config('base', dir_path=CONFIG_PATH)
            passed = True
        except Exception as e:
            passed = 'No such config file' in str(e)
            print ('s')
        assert passed

    def test_config_path(self):
        """
        """
        logger.info(" >  %s" % os.environ.get('PIPE_CONFIG_PATH'))
        test_path = os.path.join(path, 'configs')
        if not os.path.exists(test_path):
            os.mkdir(test_path)

        os.environ['PIPE_CONFIG_PATH'] = test_path

        assert os.path.exists(test_path)
        logger.info(" >  %s" % os.environ.get('PIPE_CONFIG_PATH'))
        file = os.path.join(test_path, 'other.conf')
        with open(file, 'w') as outfile:
            outfile.write("[other]\n")

    def test_config_change(self):
        test_path = os.path.join(path, 'configs')
        dir_path = os.environ['PIPE_CONFIG_PATH'] = test_path
        try:
            config('other', dir_path=dir_path)
            passed = True
        except Exception as e:
            passed = 'No such config file' in str(e)
        assert passed

        try:
            config('No_Exist', dir_path=dir_path)
            passed = False
        except Exception as e:
            passed = True
        assert passed


if __name__ == '__main__':
    unittest.main()
