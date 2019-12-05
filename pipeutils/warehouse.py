import io
import csv
import tempfile
import psycopg2 as DataPostgres
from vertica_python import connect
from pipeutils import config
from pipeutils import logger
from pipeutils.clients.client_s3 import ClientS3

VERTICA = config('vertica')
POSTGRES = config('postgres')
S3 = config('s3')


class Database(object):
    pass


class Vertica(Database):

    def __init__(self, options=None):
        self.connection = None
        options = options or {}
        self.options = {
            key: value for key, value in options.items() if value is not None
        }
        self.options.setdefault('host', VERTICA['host'])
        self.options.setdefault('port', VERTICA['port'])
        self.options.setdefault('user', VERTICA['user'])
        self.options.setdefault('database', VERTICA['database'])
        self.options.setdefault('password', VERTICA['password'])

    def connect(self):
        """
        Creates a connection or returns the current one.
        Return:
            connection
        """
        if self.connection is not None:
            logger.info(" connection: %s " % (self.connection is not None))
            if not self.connection.opened():
                logger.info("connection is closed")
                return self.reconect()

            if self.connection.opened():
                return self.connection
        try:
            self.connection = connect(**self.options)
        except Exception as e:
            logger.critical("Unable to connect to DB: {0}".format(e.message))
            raise

        return self.connection

    def close(self):
        """
        Closes any connection to vertica
        """
        if self.connection.opened():
            logger.info(' connection closed.')
            self.connection.close()

    def reconect(self):
        """
        This causes a connection reset after for every call to execute
        """
        self.connection.reset_connection()
        return self.connection

    def insert_from_csv(self, schema, table, path, delimiter=','):
        """
        Insert rows from a csv into the database.
        Required:
            schema: (str) vertica schema name
            table: (str) vertica table name
            path: (str) file path
        """
        connect = self.connect()

        if connect is not None:

            with open(path, 'rb') as fs:
                cursor = connect.cursor()
                _file = fs.read().decode('utf-8', 'ignore')
                query = "COPY {0}.{1} " \
                    "FROM STDIN " \
                    "PARSER FDELIMITEDPARSER " \
                    "(delimiter='{2}')".format(schema, table, delimiter)
                cursor.copy(query, _file)
                connect.commit()

    def insert_from_dataframe(self, schema, table, dataframe):
        """
        Insert rows from a dataframe into of database vertica.
        Required:
            schema: (str) vertica schema name
            table: (str) vertica table name
            dataframe: (pd) Data Frame
        """
        connect = self.connect()
        if connect is not None:
            cursor = connect.cursor()
            csv_buf = io.StringIO()
            dataframe.to_csv(csv_buf, header=False, index=None,
                             encoding='utf-8', quoting=csv.QUOTE_MINIMAL,
                             escapechar='\\')
            query = "COPY {0}.{1} ({2}) " \
                "FROM STDIN " \
                "DELIMITER AS '{3}'" \
                "DIRECT ABORT ON " \
                "ERROR NO COMMIT;".format(schema,
                                          table,
                                          ', '.join(dataframe.columns), ",")
            cursor.copy(query, csv_buf.getvalue())
            connect.commit()

    def insert_from_s3(self, schema, table, path):
        """
        Insert rows from a dataframe into of database vertica.
        Required:
            schema: (str) vertica schema name
            table: (str) vertica table name
            th: (str) Path file in s3
        """
        client_s3 = ClientS3(S3['bucket'])
        temp = tempfile.NamedTemporaryFile()
        temp.close()
        client_s3.download(path, temp.name)

        try:
            self.insert_from_csv(schema, table, temp.name)
        except Exception as e:
            logger.error("No found: {0}".format(e.message))


class Postgres(Database):

    def __init__(self, options=None):
        self.connection = None
        options = options or {}
        self.options = {
            key: value for key, value in options.items() if value is not None
        }
        self.options.setdefault('host', POSTGRES['host'])
        self.options.setdefault('port', POSTGRES['port'])
        self.options.setdefault('user', POSTGRES['user'])
        self.options.setdefault('database', POSTGRES['database'])
        self.options.setdefault('password', POSTGRES['password'])

    def connect(self):
        """
        Creates a connection or returns the current one.
        Return:
            connection
        """
        if self.connection is not None:
            logger.info(" connection: %s " % (self.connection is not None))
            return self.connection
        try:
            self.connection = DataPostgres.connect(**self.options)
        except Exception as e:
            logger.critical("Unable to connect to DB: {0}".format(e.message))
            raise

        return self.connection

    def close(self):
        """
        Closes any connection to vertica
        """
        logger.info(' connection closed.')
        return self.connection.close()

    def reconect(self):
        """
        This causes a connection reset after for every call to execute
        """
        self.connection = DataPostgres.connect(**self.options)
        return self.connection

    def copy_expert(self, table, path, delimiter=';'):
        """
        Insert rows from a csv into the database.
        Required:
            table: (str) vertica table name
            path: (str) file path
        """
        connect = self.connect()

        if connect is not None:
            with open(path, 'r') as fs:
                query = "COPY {} " \
                        "FROM STDIN " \
                        "WITH CSV HEADER DELIMITER '{}'".format(table,
                                                                delimiter)
                with connect.cursor() as cur:
                    cur.copy_expert(sql=query, file=fs)
                    connect.commit()
                    cur.close()
                    connect.close()
        return True
