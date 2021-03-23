import unittest
import os
import logging
from pipeutils import config
from pipeutils import logger
from . import PATH_CONFIG


logger.setLevel(logging.DEBUG)


class TestConfigFile(unittest.TestCase):
    def test_config(self):
        failed = False
        try:
            config('base', path=PATH_CONFIG)
        except Exception as e:
            logger.error(f"Exception {e}")
            failed = True
        self.assertFalse(failed)


if __name__ == '__main__':
    unittest.main()
