import os
import io

from pipeutils import logger
from pipeutils import config
from mimetypes import MimeTypes

from httplib2 import Http
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
from apiclient.discovery import build
from oauth2client import client
from oauth2client import tools
from oauth2client import file


GDRIVE = config('gdrive')


class GDrive(object):

    def __init__(self, **args):
        self.creds = self.get_credentials()
        self.service = self.initialize_service()

    def get_credentials(self, **args):
        credential_path = os.path.join(GDRIVE['secret_file'])
        store = file.Storage(credential_path)
        creds = store.get()
        if not creds or creds.refresh(creds.authorize(Http())):
            flow = client.flow_from_clientsecrets(
                GDRIVE['secret_file'], GDRIVE['scopes']
            )
            creds = tools.run(flow, store)
        return creds

    def initialize_service(self):
        self.creds = self.get_credentials()
        http = self.creds.authorize(Http())
        return build('drive', 'v3', http=http)

    def get_folder_id(self, name):
        results = self.service.files().list(
            pageSize=10,
            q=("name = '{0}'".format(name) +
               " and mimeType = 'application/vnd.google-apps.folder'"),
            corpora="user",
            fields="nextPageToken, files(id, name, webContentLink, " +
                   "createdTime, modifiedTime)").execute()
        item = results.get('files', [])
        logger.info(item)
        if not item:
            return None
        return item[0]['id']

    def listfiles(self):
        items = results.get('files', [])
        if not items:
            logger.info('No files found.')
        else:
            return items

    def upload(self, path, gpath):
        mime = MimeTypes()
        file_metadata = {
            'name': os.path.basename(path),
        }
        if self.get_item_id(gpath) is not None:
            file_metadata['parents'] = [self.get_folder_id(gpath)]

        media = MediaFileUpload(path,
                                mimetype=mime.guess_type(
                                    os.path.basename(path))[0],
                                resumable=True)
        id_file = []
        try:
            file = self.service.files().create(body=file_metadata,
                                               media_body=media,
                                               fields='id').execute()
            id_file = file.get('id')
        except HttpError:
            logger.info('corrupted file')
            pass
        return id_file

    def download(self, gpath, path):
        if self.listfiles() is not None:
            items = self.listfiles()
            for item in items:
                if gpath in item['name']:
                    request = self.service.files().get_media(fileId=item['id'])
                    fh = io.BytesIO()
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while done is False:
                        status, done = downloader.next_chunk()
                        logger.info(int(status.progress() * 100))

                    f = open(path + '/' + item['name'], 'wb')
                    f.write(fh.getvalue())
                    f.close()
        else:
            logger.info('No files found.')
