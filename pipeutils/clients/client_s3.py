import os
import boto3
import botocore

from pipeutils import logger
from pipeutils import config

s3 = config('s3')


class ClientS3(object):
    session = boto3.Session(aws_access_key_id=s3['aws_access_key_id'],
                            aws_secret_access_key=s3['aws_secret_access_key'])
    clientS3 = session.resource('s3')

    def __init__(self, bucket):
        self.bucket = bucket

    def upload(self, path, s3path):
        '''
        Args:
            path (str): The `path` of file.
            s3path (str): The s3path direction in bucket into s3
        '''
        try:
            data = open(path, 'rb')
            self.clientS3.Bucket(self.bucket).put_object(Key=s3path, Body=data)
            data.close()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                logger.error('The object does not exist.')
            else:
                raise

    def upload_multiple(self, path, s3path, extension=None):
        '''
        Args:
            path (str): the directory path of the file.
            s3path (str): the s3path direction directory in the bucket into s3.
            extension (str): the extension of file to upload.
        '''
        for name_file in os.listdir(path):
            if extension is None or extension in name_file:
                source_path =  os.path.join(path, name_file)
                s3_path = os.path.join(s3path, name_file)
                self.upload(source_path, s3_path)


    def download(self, s3path, path):
        '''
        Args:
            s3path (str): The s3path direction in bucket into s3
            path (str): The `path` of file where will it be download.
        '''
        try:
            self.clientS3.Bucket(self.bucket).download_file(s3path, path)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                logger.error('The object does not exist.')
            else:
                raise

    def list(self):
        """
        List objects into bucket
        """
        objects = []
        bucket = self.clientS3.Bucket(self.bucket)
        for s3_file in bucket.objects.all():
            logger.info('File > %s' % s3_file.key)
            objects.append(s3_file.key)
        return objects
