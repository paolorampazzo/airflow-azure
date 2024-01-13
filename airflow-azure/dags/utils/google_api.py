import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from airflow.models import Variable

from os import makedirs
from os.path import join

json_data = Variable.get('google_json_password')
credentials_path = '/mnt/mydata/credentials'
credentials_filename = join(credentials_path, 'credentials.json')
try:
    makedirs(credentials_path)
except:
    pass

with open(credentials_filename, 'w') as f:
    f.write(json_data)

# Define the Google Drive API scopes and service account file path
SCOPES = ['https://www.googleapis.com/auth/drive']
# SERVICE_ACCOUNT_FILE = "/file/path/of/json/file.json"
SERVICE_ACCOUNT_FILE = credentials_filename

# Create credentials using the service account file
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Build the Google Drive service
drive_service = build('drive', 'v3', credentials=credentials)

def create_folder(folder_name, parent_folder_id=None):
    """Create a folder in Google Drive and return its ID."""
    folder_metadata = {
        'name': folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        'parents': [parent_folder_id] if parent_folder_id else []
    }

    created_folder = drive_service.files().create(
        body=folder_metadata,
        fields='id'
    ).execute()

    print(f'Created Folder ID: {created_folder["id"]}')
    return created_folder["id"]

def list_folder(parent_folder_id=None, delete=False):
    """List folders and files in Google Drive."""
    results = drive_service.files().list(
        q=f"'{parent_folder_id}' in parents and trashed=false" if parent_folder_id else None,
        pageSize=1000,
        fields="nextPageToken, files(id, name, mimeType)"
    ).execute()
    items = results.get('files', [])

    return items

    # if not items:
    #     print("No folders or files found in Google Drive.")
    # else:
    #     print("Folders and files in Google Drive:")
    #     for item in items:
    #         print(f"Name: {item['name']}, ID: {item['id']}, Type: {item['mimeType']}")
    #         if delete:
    #             delete_files(item['id'])

def delete_files(file_or_folder_id):
    """Delete a file or folder in Google Drive by ID."""
    try:
        drive_service.files().delete(fileId=file_or_folder_id).execute()
        print(f"Successfully deleted file/folder with ID: {file_or_folder_id}")
    except Exception as e:
        print(f"Error deleting file/folder with ID: {file_or_folder_id}")
        print(f"Error details: {str(e)}")

def download_file(file_id, destination_path):
    """Download a file from Google Drive by its ID."""
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.FileIO(destination_path, mode='wb')
    
    downloader = MediaIoBaseDownload(fh, request)
    
    done = False
    while not done:
        status, done = downloader.next_chunk()
        print(f"Download {int(status.progress() * 100)}%.")

import os

def create_folder_with_file(folder_name, file_path, credentials_path, parent_folder_id=None):
    """Create a folder in Google Drive and upload a local file to it."""
    # Step 1: Create the folder
    folder_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_folder_id] if parent_folder_id else []
    }

    created_folder = drive_service.files().create(
        body=folder_metadata,
        fields='id'
    ).execute()

    folder_id = created_folder['id']
    print(f'Created Folder ID: {folder_id}')

    # Step 2: Upload the local file to the created folder
    file_metadata = {'name': os.path.basename(file_path), 'parents': [folder_id]}

    media = MediaFileUpload(file_path, resumable=True)

    uploaded_file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()

    file_id = uploaded_file['id']
    print(f'Uploaded File ID: {file_id}')

    return folder_id, file_id
