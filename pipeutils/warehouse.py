import io
import csv
from vertica_python import connect
from pipeutils import config
from pipeutils import logger

ASCII = 'ascii'
try:
    VERTICA = config('vertica')
except Exception as e:
    logger.info('error config file.')

class Database(object):

    def __init__(self, options=None):
        self.connection = None
        options = options or {}
        self.options = {key: value for key, value in options.items() if value is not None}
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
        if self.connection is None:
            try:
                self.connection = connect(**self.options)
            except Exception as e:
                logger.critical("Unable to connect to DB: {0}".format(e.message))
                raise

        if self.connection is not None and self.connection.opened():
            return self.connection

        return self.connection

    def close(self):
        if self.connection.opened():
            logger.info(' connection closed.')
            self.connection.close()

    def reconect(self):
        self.connection.reset_connection()
        return self.connection


class Vertica(Database):

    def __init__(self):
        pass

    def insert(self, schema, table, rows):
        pass

    def insert_from_csv(self, schema, table, path):
        connect = self.connect()

        if connect is not None:

            with open(path, 'rb') as fs:
                cursor = connect.cursor()
                _file = fs.read().decode('utf-8', 'ignore')
                query = "COPY {0}.{1} " \
                        "FROM STDIN " \
                        "PARSER FDELIMITEDPARSER " \
                        "(delimiter=',')".format(schema, table)
                cursor.copy(query, _file)
                connect.commit()

    def insert_from_dataframe(self, schema, table, dataframe):
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
                    "DIRECT ABORT ON ERROR NO COMMIT;".format(schema, table,
                                                              ', '.join(dataframe.columns), ",")
            cursor.copy(query, csv_buf.getvalue())
            connect.commit()


    def insert_from_s3(self, schema, table, s3path, filter='*.csv'):
        pass
    def update(self, schema, table, rows):
        pass
    def update_from_csv(self, schema, table, path):
        pass
    def update_from_dataframe(self, schema, table, dataframe):
        pass
    def update_from_s3(self, schema, table, s3path, filter='*.csv'):
        pass
