import unittest
import os
import logging
import fnmatch
from os.path import isfile, join
from pipeutils.warehouse import (
    Postgres
)

path = os.path.dirname(os.path.realpath(__file__))


class TestClientPostgres(unittest.TestCase):

    def test_connect(self):
        """
        Check that attempting to open a existent connection
        """
        client = Postgres()
        conn = client.connect()
        logging.info(conn)
        client.close()
        """
        Check that attempting to reopen a existent connection
        and check exist schema in database.
        """
        conn2 = client.reconect()
        cursor = conn2.cursor()
        cursor.execute('SELECT schema_name FROM information_schema.schemata')
        result = cursor.fetchall()
        logging.info(result)
        self.assertIsNotNone(result)

    def test_load_csv(self):
        """
          Verify that you try to load the database from csv files.
        """
        client = Postgres()
        data_path = os.path.join(path, 'data')

        conn = client.connect()
        cursor = conn.cursor()
        cursor.execute('DROP TABLE test_job')
        sql = 'CREATE TABLE test_job("id" text NOT NULL PRIMARY KEY, ' \
              '"job_id" varchar(256) NOT NULL UNIQUE, '\
              '"link" varchar(256) NOT NULL, '\
              '"title" text NOT NULL, ' \
              '"extracted_at" timestamp NOT NULL, ' \
              '"extras" varchar(256) NOT NULL, '\
              '"location" varchar(256) NOT NULL, ' \
              '"description" text NOT NULL, ' \
              '"employer_id" varchar(256) NULL, ' \
              '"source" varchar(256) NULL);'
        cursor.execute(sql)
        cursor.execute("select count(id) from test_job;")
        result = cursor.fetchall()

        if os.path.exists(data_path):
            files = [f for f in os.listdir(data_path)
                     if isfile(join(data_path, f))]
            for file in files:
                if file == 'file_test.csv':
                    logging.info(file.encode('utf-8'))
                    if fnmatch.fnmatch(file, '*.csv'):
                        _file = os.path.join(data_path, file)
                        Postgres.insert_from_csv(client, 'test_job',
                                                 _file)
        cursor.execute("select count(id) from test_job;")
        result_2 = cursor.fetchall()
        self.assertNotEqual(result[0], result_2[0])


if __name__ == '__main__':
    unittest.main()
