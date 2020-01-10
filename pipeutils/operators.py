from pipeutils import config
from pipeutils.clients.client_s3 import ClientS3
from pipeutils.warehouse import Postgres
from pipeutils import logger
from airflow.models import BaseOperator

s3 = config('s3')


class XComParams(BaseOperator):

    def __init__(self, lookups={}, *args, **kwargs):
        """
        Operator to make accesible xcom params in `self.dag_params`.
        The `lookup` param, maps how the xcom params are going to be stored.
        It uses the keys as param keys in self.dag_params and the values as
        the taskids.
        I.E::
            t = XComParams(lookups={'var1': 'taskid_1'})

        In function `execute` the `self.dag_params` var will be populated as
        a dictionary:

        >>> t.dag_params
        {'var1': 'what taskid_id task returned in `execute` function'}
        """
        logger.info(kwargs)
        self.lookups = lookups
        BaseOperator.__init__(self, *args, **kwargs)

    def pre_execute(self, context):
        self.dag_params = {}

        for param, task_id in self.lookups.items():
            task_instance = context['task_instance']
            self.dag_params[param] = task_instance.xcom_pull(task_ids=task_id)


class UploadToS3(XComParams):

    def __init__(self, destination, extension, source=None, *args, **kwargs):
        """
        Operator to upload all files with file extension `extension`
        (if defined) in s3 from `source` directory to s3 path `destination`.

        if `source` or `destination` is None, they should be accesible
        through `dag_params` so `lookups` need to be explicit defined for that.
        Args:
            source (str): path to the directory to find the files to upload
            into s3.
            destination (str): path in s3 to store the files.
            extension (str): filter the files to be uploaded by extension.
        """

        super(UploadToS3, self).__init__(*args, **kwargs)
        self.source = source
        self.destination = destination
        self.extension = extension

    def execute(self, context):
        if self.source is None:
            self.source = self.dag_params['source']

        logger.info(self.destination)
        s3_client = ClientS3(s3['bucket'])
        result = s3_client.upload_multiple(self.source, self.destination,
                                           self.extension)
        logger.ingo(result)
        logger.info("The files has been saved successfully")
        return self.source


class PopulatePostgres(XComParams):

    def __init__(self, table, source=None, *args, **kwargs):
        """
        Insert data into table `{table}` all csv files found in
        source, source should be accesible thourhg `self.dag_params`
        if not passed.
        Args:
            table (str): Vertica table name
            source (str): path to csv file, in case of None the class
            should look at `dag_params`
        """
        """
        Save the csv path into the Vertica table jobs.jobs
        """

        logger.info("Starting to save the CSV to Vertica")

        super(PopulatePostgres, self).__init__(*args, **kwargs)
        self.source = source
        self.table = table

    def execute(self, context):
        if self.source is None:
            self.source = self.dag_params['source']
        # start the Postgres client
        client = Postgres()
        # Save the data into the Postgres from the csv path
        result = Postgres.copy_expert(client, self.table,
                                      self.source)
        logger.info("The CSV has been saved successfully")
        return result
