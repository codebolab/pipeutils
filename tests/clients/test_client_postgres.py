import unittest
import os
from pipeutils.warehouse import (
    Postgres
)

path = os.path.dirname(os.path.realpath(__file__))

SQL = 'CREATE TABLE IF NOT EXISTS test_table("id" text NOT NULL PRIMARY KEY, '\
      '"job_id" varchar(256));'


class TestClientPostgres(unittest.TestCase):

    def setUp(self):
        self.client = Postgres()
        self.conn = self.client.connect()
        self.cursor = self.conn.cursor()
        self.cursor.execute('DROP TABLE IF EXISTS test_table')
        self.cursor.execute(SQL)
        self.conn.commit()

    def tearDown(self):
        self.client.reconect()
        self.cursor.execute('DROP TABLE IF EXISTS test_table')
        self.client.close()

    def test_load_csv(self):
        """
        Verify that you try to load the database from csv files.
        """
        # load file with contend id "a3efd6e0"
        csv_to_load = os.path.join(path, 'data', 'file_test.csv')

        self.client.reconect()
        result = self.client.copy_expert('test_table', csv_to_load)

        self.assertEqual(csv_to_load, result)

        self.client.reconect()
        self.cursor.execute("select * from test_table limit 1;")
        records = self.cursor.fetchall()
        self.assertEqual(records[0][0], 'a3efd6e0')


if __name__ == '__main__':
    unittest.main()
