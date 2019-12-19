import unittest
import os
import logging
from pipeutils.warehouse import (
    Postgres
)

path = os.path.dirname(os.path.realpath(__file__))


class TestClientPostgres(unittest.TestCase):

    def setUp(self):
        self.client = Postgres()
        self.conn = self.client.connect()
        self.cursor = self.conn.cursor()

    def tearDown(self):
        conn = self.client.reconect()
        cursor = conn.cursor()
        cursor.execute('DROP TABLE test_job')
        self.client.close()

    def test_load_csv(self):
        """
        Verify that you try to load the database from csv files.
        """
        csv_to_load = os.path.join(path, 'data', 'file_test.csv')
        self.cursor.execute('DROP TABLE test_job')

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

        self.cursor.execute(sql)
        self.cursor.execute("select count(*) from test_job;")
        empty_records = self.cursor.fetchall()
        logging.info(csv_to_load.encode('utf-8'))
        result = self.client.copy_expert('test_job', csv_to_load)

        self.assertEqual(csv_to_load, result)

        conn = self.client.reconect()
        cursor = conn.cursor()
        cursor.execute("select count(*) from test_job;")
        all_records = cursor.fetchall()
        self.assertNotEqual(empty_records[0], all_records[0])

        cursor.execute("select * from test_job limit 1;")
        records = cursor.fetchall()
        self.assertEqual(records[1], 'a3efd6e0')


if __name__ == '__main__':
    unittest.main()
