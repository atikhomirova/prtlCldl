import logging
from airflow.plugins_manager import AirflowPlugin
from airflow.models import Variable
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from googleapiclient.discovery import build
from airflow.utils.log.logging_mixin import LoggingMixin
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import io
from os.path import expanduser, exists, basename

logging.getLogger("google_drive").setLevel(logging.ERROR)

LOCAL_FILE_PATH = expanduser('~') + '/gcs/data/'


class GoogleDriveHook(GoogleCloudBaseHook, LoggingMixin):
    """
    Interact with Google Spreadsheets

    :param spreadsheet_id: The spreadsheet id
    :type spreadsheet_id: string
    :param spreadsheet_range: The spreadsheet range
    :type spreadsheet_range: string
    :param drive_conn_id: The Airflow connection ID to download with
    :type drive_conn_id: string
    :param delegate_to: The account to impersonate, if any
    :type delegate_to: string
    """

    def __init__(self, file_id='',
                 targ_file_name='processed_file',
                 drive_conn_id="google_cloud_default",
                 delegate_to=None):

        super(GoogleDriveHook, self).__init__(
            conn_id=drive_conn_id, delegate_to=delegate_to)

        self.file_id = file_id
        self.targ_file_name = targ_file_name
        self.valid_mimeTypes = {'application/vnd.ms-excel': '.xls',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': '.xlsx',
            'application/vnd.ms-excel.sheet.binary.macroenabled.12': '.xlsb',
            'text/csv': '.csv',
            'text/plain': '.txt'
            }


    def get_service(self):
        """
        Returns a Sheets service object.
        """
        http_authorized = self._authorize()
        return build('drive', 'v3', http=http_authorized)


    def get_mimetype(self, file_id):
        service = self.get_service()
        file = service.files().get(
            fileId=file_id, supportsTeamDrives=True).execute()
        return file.get('mimeType')


    def get_filename(self, file_id):
        service = self.get_service()
        file = service.files().get(
            fileId=file_id, supportsTeamDrives=True).execute()
        return file.get('name')


    def list_files(self):
        service = self.get_service()
        result = service.files().list(
            # spaces='drive',
            corpora='teamDrive',
            teamDriveId='0ALef8fXDatdIUk9PVA',
            supportsTeamDrives=True,
            includeTeamDriveItems=True,
            pageSize=100,
            fields="files(id, name, mimeType, parents)").execute()
        return result.get('files', [])


    def get_file_IDs_list(self, parentFolderId):
        service = self.get_service()
        fileIDsList = []
        page_token = None
        while True:
            response = service.files().list(
                #q="'%s' in parents and trashed=false" % parentFolderId,
                # spaces='drive',
                # corpora='teamDrive',
                corpora='user',
                #teamDriveId='0ALef8fXDatdIUk9PVA',
                supportsTeamDrives=True,
                includeTeamDriveItems=True,
                pageSize=100,
                fields="nextPageToken, files(id, name, mimeType, parents)"
            ).execute()
            for file in response.get('files', []):
                #logging.info('Found file: %s (%s)' % (file.get('name'), file.get('id')))
                if file.get('mimeType') in self.valid_mimeTypes:
                    fileIDsList.append(file.get('id'))
                    logging.info('Added File ID: %s (%s)' % (file.get('name'), file.get('id')))
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break
        if fileIDsList is None:
            logging.info('No files are available')
        return fileIDsList


    def get_file(self, file_id):
        service = self.get_service()
        #file_mimeType = self.get_mimetype(file_id)
        #logging.info('File mime type is:', file_mimeType)
        file_name = self.get_filename(file_id)
        #logging.info('File name is:', file_name)

        #file_extension = self.valid_extensions[file_mimeType]

        result = service.files().get_media(fileId=file_id)

        with open(LOCAL_FILE_PATH + self.targ_file_name + '_' + file_name,
            'wb') as f:
            f.write(result.execute())
        logging.info('File ' + file_name + ' is loaded')
        #return file_mimeType


    def upload_file(self, file_path, dest_folder_id):
        service = self.get_service()

        file_metadata = {
            'name': basename(file_path),
            'teamDriveId': '0ALef8fXDatdIUk9PVA',
            'parents': [dest_folder_id]
        }
        media = MediaFileUpload(file_path, mimetype='text/csv')
        result = service.files().create(
            body=file_metadata, media_body=media,
            supportsTeamDrives=True).execute()


    def get_changes(self):
        service = self.get_service()
        saved_start_page_token = Variable.get("TokenForDriveChanges")
        filesListVar = ''
        page_token = saved_start_page_token
        while page_token is not None:
            response = service.changes().list(
                pageToken=page_token,
                teamDriveId='0ALef8fXDatdIUk9PVA',
                supportsTeamDrives=True,
                includeTeamDriveItems=True,
                pageSize=100).execute()
            for change in response.get('changes'):
                if change.get('fileId') is not None:
                    filesListVar = filesListVar + change.get('fileId') + ','
                #logging.info('Change found for file: %s' % change.get('fileId'))
            if 'newStartPageToken' in response:
                # Last page, save this token for the next polling interval
                saved_start_page_token = response.get('newStartPageToken')
            page_token = response.get('nextPageToken')
        Variable.set("TokenForDriveChanges", saved_start_page_token)
        if filesListVar != '':
            filesListVar = filesListVar[:-len(',')]
        Variable.set("ListOfChangedFiles", filesListVar)


    def list_folders(self, mimeType="application/vnd.google-apps.folder"):
        service = self.get_service()
        folder = []
        result = service.files().list(
            # spaces='drive',
            corpora='teamDrive',
            teamDriveId='0ALef8fXDatdIUk9PVA',
            supportsTeamDrives=True,
            includeTeamDriveItems=True,
            pageSize=100,
            fields="files(id, name, mimeType, parents)").execute()
        folders = result.get('files', [])
        for fold in folders[:]:
            if fold["mimeType"] == mimeType:
                del fold["mimeType"]
                folder.append(fold)

        return folder


class AirflowTestPlugin(AirflowPlugin):
    name = "gdrive_hook"
    hooks = [GoogleDriveHook]
