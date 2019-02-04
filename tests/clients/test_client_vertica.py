import unittest
import os
import logging
import pandas as pd
import fnmatch
from pipeutils.warehouse import Database, Vertica
from os.path import isfile, join

path = os.path.dirname(os.path.realpath(__file__))


class TestClientVertica(unittest.TestCase):

    def test_connect(self):
        """
        Check that attempting to open a existent connection
        """
        client = Database()
        conn = client.connect()
        client.close()
        """
        Check that attempting to reopen a existent connection
        and check exist schema in database. 
        """
        conn = client.reconect()
        cursor = conn.cursor()
        cursor.execute("select name from test.example")
        result = cursor.fetchall()
        logging.info(result)
        self.assertIsNone(result)

    def test_load_csv(self):
        """
          Verify that you try to load the database from csv files.
        """
        client = Database()
        data_path = os.path.join(path, 'data')

        conn = client.connect()
        cursor = conn.cursor()
        cursor.execute("select count(name) from test.fb_user_bio;")
        result = cursor.fetchall()

        if os.path.exists(data_path):
            files = [f for f in os.listdir(data_path) if isfile(join(data_path, f))]
            for file in files:
                logging.info(file.encode('utf-8'))
                if fnmatch.fnmatch(file, '*.csv'):
                    _file = os.path.join(data_path, file)
                    Vertica.insert_from_csv(client, 'test', 'fb_user_bio', _file)
        cursor.execute("select count(name) from test.fb_user_bio;")
        result_2 = cursor.fetchall()
        self.assertNotEqual(result[0], result_2[0])

    def test_load_frame(self):
        """
          Verify that you try to load the database from dataframe.
        """
        client = Database()

        conn = client.connect()
        cursor = conn.cursor()
        cursor.execute("select count(name) from test.fb_user_bio;")
        result = cursor.fetchall()

        col_names = ['name', 'param', 'value', 'fb_id_user']

        pdata = pd.DataFrame(columns=col_names, index=None)

        pdata.loc[len(pdata)] = ['CBA (Centro Boliviano Americano)', 'Education',
                                 'https://www.facebook.com/CBA.SC/', '1401639694']
        Vertica.insert_from_dataframe(client, 'test', 'fb_user_bio', pdata)

        cursor.execute("select count(name) from test.fb_user_bio;")
        result_2 = cursor.fetchall()
        self.assertEqual(sum(result[0]), sum(result_2[0])-1)


if __name__ == '__main__':
    unittest.main()