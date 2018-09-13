import unittest
import os
import logging

from __init__ import config


class TestConfigFile(unittest.TestCase):

    def test_config(self):
        """
        Check that attempting to open a non-existent config file raises
        an Exception with appropriate message.
        """

        passed = False
        try:
            c = config('base')
            passed = True
        except Exception as e:
            passed = 'No such config file' in str(e)
        assert passed


if __name__ == '__main__':
    unittest.main()
