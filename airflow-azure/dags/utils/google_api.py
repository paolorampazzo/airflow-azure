import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from airflow.models import Variable

from os import makedirs
from os.path import join

credentials_path = '/mnt/mydata/credentials'
credentials_filename = join(credentials_path, 'credentials.json')

def generate_credentials():
  json_data = Variable.get('google_json_password')
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

  return drive_service

def create_folder(folder_name, parent_folder_id=None):
    """Create a folder in Google Drive and return its ID."""

    drive_service = generate_credentials()

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
    drive_service = generate_credentials()

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

    drive_service = generate_credentials()

    try:
        drive_service.files().delete(fileId=file_or_folder_id).execute()
        print(f"Successfully deleted file/folder with ID: {file_or_folder_id}")
    except Exception as e:
        print(f"Error deleting file/folder with ID: {file_or_folder_id}")
        print(f"Error details: {str(e)}")

def download_file(file_id, destination_path):
    """Download a file from Google Drive by its ID."""

    drive_service = generate_credentials()

    request = drive_service.files().get_media(fileId=file_id)
    fh = io.FileIO(destination_path, mode='wb')
    
    downloader = MediaIoBaseDownload(fh, request)
    
    done = False
    while not done:
        status, done = downloader.next_chunk()
        print(f"Download {int(status.progress() * 100)}%.")

import os

def create_folder_with_file(folder_name, file_path, parent_folder_id=None):
    """Create a folder in Google Drive and upload a local file to it."""

    drive_service = generate_credentials()

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

def upload_file(file_path, parent_folder_id=None):
    """Create a folder in Google Drive and upload a local file to it."""

    drive_service = generate_credentials()

    # Step 2: Upload the local file to the created folder
    file_metadata = {'name': os.path.basename(file_path), 'parents': [parent_folder_id]}

    media = MediaFileUpload(file_path, resumable=True)

    uploaded_file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()

    file_id = uploaded_file['id']
    print(f'Uploaded File ID: {file_id}')

    return file_id

def update_file(file_path, file_id):
    """Create a folder in Google Drive and upload a local file to it."""

    drive_service = generate_credentials()

    media = MediaFileUpload(file_path, resumable=True)

    updated_file = drive_service.files().update(fileId=file_id,
    media_body=media).execute()

    file_id = updated_file['id']
    print(f'Updated File ID: {file_id}')

    return file_id

def send_to_drive(version, course_folder, parent_folder_id, file_path, filename, overwrite = False, error=False):

    print('filepath =', file_path)
    print('filename =', filename)

    folder_name = f'Zach-{version}'
    folders = list_folder(parent_folder_id)

    zach_folder_id = ''

    for folder in folders:
        if folder_name == folder['name']:
            zach_folder_id = folder['id']
            print('found', zach_folder_id)
        
    print('folders', folders)
    if not zach_folder_id:
        zach_folder_id = create_folder(folder_name, parent_folder_id)
        print(zach_folder_id)   
        create_folder_with_file(course_folder, file_path, zach_folder_id)
        return

    folders_in_zach_folder = list_folder(zach_folder_id)

    if error:
        for folder in folders_in_zach_folder:
            if 'Errors' == folder['name']:
                zach_folder_id = folder['id']
                print('error found', zach_folder_id)   
                 
        folders_in_zach_folder = list_folder(zach_folder_id)

    
                

    course_name_folder_id = ''

    for folder in folders_in_zach_folder:
        if course_folder == folder['name']:
            course_name_folder_id = folder['id']
            print(course_folder)
            print('found', course_name_folder_id)    
    
    if not course_name_folder_id:
        create_folder_with_file(course_folder, file_path, zach_folder_id)
    else:
        if overwrite:

            file_id = [file['id'] for file in list_folder(course_name_folder_id) if file['name'] == filename][0]
            update_file(file_path, file_id)
        else:
            upload_file(file_path, course_name_folder_id)

    