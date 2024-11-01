"""

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Audio Transcription Pipeline for Echelon NOS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Authors: Christian Bader
October 2024 - Present

This script iterates through audio/video files in a Google Drive folder and transcribes them. 
Audio files are saved as .mp3 files in a new folder, Transcripts are saved as .docx in a new folder.
.docx files are assigned metadata properties like transcription date/time, seconds transcribed, etc.
Custom properties set on a file in Google Drive using the API are not visible through the 
Google Drive web interface. To access these properties, you need to use the Google Drive API.

"""

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import streamlit as st

# Define functions that interact with Google Docs + Drive

def initalize_google_services(scopes, creds)
    creds = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=SCOPES
)
drive_service = build('drive', 'v3', credentials=creds)
docs_service = build('docs', 'v1', credentials=creds)

def gd_list_audio_video_files(folder_id):
    """
    Lists all audio and video files in a Google Drive folder.

    Parameters:
        folder_id (str): The ID of the Google Drive folder.

    Returns:
        list: A list of files with their 'id', 'name', and 'mimeType'.
    """
    query = f"'{folder_id}' in parents and (mimeType contains 'audio/' or mimeType contains 'video/')"
    results = drive_service.files().list(q=query, fields="files(id, name, mimeType)").execute()
    files = results.get('files', [])
    return files


def gd_download_file(file_id, file_name):
    """
    Downloads a file from Google Drive.

    Parameters:
        file_id (str): The ID of the file to download.
        file_name (str): The name to save the file as locally.

    Returns:
        str: The local path to the downloaded file.
    """
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_name, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        print(f"Download {file_name}: {int(status.progress() * 100)}%.")
    return file_name


def gd_upload_file(file_path, folder_id, mime_type):
    """
    Uploads a file to a specified Google Drive folder.

    Parameters:
        file_path (str): The local path to the file to upload.
        folder_id (str): The ID of the destination Google Drive folder.
        mime_type (str): The MIME type of the file.

    Returns:
        str: The ID of the uploaded file in Google Drive.
    """
    file_metadata = {
        'name': os.path.basename(file_path),
        'parents': [folder_id]
    }

    media = MediaFileUpload(file_path, mimetype=mime_type)
    uploaded_file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()
    return uploaded_file.get('id')


def gd_get_file_properties(file_id):
    """
    Retrieves the properties of a file from Google Drive.

    Parameters:
        file_id (str): The ID of the file.

    Returns:
        dict: A dictionary containing the file's properties.
    """
    file = drive_service.files().get(fileId=file_id, fields='properties').execute()
    properties = file.get('properties', {})
    return properties


def gd_merge_file_properties(file_id, new_properties):
    """
    Merges the properties of a file in Google Drive.

    Parameters:
        file_id (str): The ID of the file.
        new_properties (dict): A dictionary of new properties to set.

    Returns:
        dict: The updated file resource.
    """
    file_metadata = {
        'properties': new_properties
    }
    updated_file = drive_service.files().update(
        fileId=file_id,
        body=file_metadata,
        fields='id, properties'
    ).execute()
    return updated_file


def gd_update_file_properties(file_id, new_properties):
    """
    Clears all existing properties of a file in Google Drive and sets new properties.

    Parameters:
        file_id (str): The ID of the file.
        new_properties (dict): A dictionary of new properties to set.

    Returns:
        dict: The updated file resource.
    """
    # Step 1: Retrieve existing properties
    file = drive_service.files().get(fileId=file_id, fields='properties').execute()
    existing_properties = file.get('properties', {})

    # Step 2: Prepare properties to delete (set their values to None)
    properties_to_delete = {key: None for key in existing_properties.keys()}

    # Step 3: Combine properties to delete with new properties
    update_properties = {**properties_to_delete, **new_properties}

    # Step 4: Update the file properties
    file_metadata = {
        'properties': update_properties
    }
    updated_file = drive_service.files().update(
        fileId=file_id,
        body=file_metadata,
        fields='id, properties'
    ).execute()
    return updated_file

def gd_move_file_between_folders(file_id, target_folder_id):
    """
    Moves a file to a different Google Drive folder.

    Parameters:
        file_id (str): The ID of the file to move.
        target_folder_id (str): The ID of the destination folder.

    Returns:
        None
    """
    try:
        # Retrieve the existing parents to remove
        file = drive_service.files().get(fileId=file_id, fields='parents').execute()
        previous_parents = ",".join(file.get('parents'))

        # Move the file to the new folder
        drive_service.files().update(
            fileId=file_id,
            addParents=target_folder_id,
            removeParents=previous_parents,
            fields='id, parents'
        ).execute()

        print(f"File ID {file_id} moved to folder ID {target_folder_id}")
    except Exception as e:
        print(f"Error moving file {file_id}: {str(e)}")


def gd_get_shareable_link(file_id):
    """
    Creates a shareable link for a Google Drive file.

    Parameters:
        file_id (str): The ID of the file.

    Returns:
        str: The shareable link to the file.
    """
    try:
        # Update file permissions to make it shareable
        permission = {
            'type': 'anyone',
            'role': 'reader'
        }
        drive_service.permissions().create(fileId=file_id, body=permission).execute()

        # Get the shareable link
        file = drive_service.files().get(fileId=file_id, fields='webViewLink').execute()
        return file.get('webViewLink')
    except Exception as e:
        print(f"Error getting shareable link for file {file_id}: {str(e)}")
        return None

def gd_rename_file(file_id, new_name):
    """
    Renames a file in Google Drive.

    Parameters:
        file_id (str): The ID of the file to rename.
        new_name (str): The new name for the file.

    Returns:
        dict: The updated file resource.
    """
    try:
        file_metadata = {'name': new_name}
        updated_file = drive_service.files().update(
            fileId=file_id,
            body=file_metadata,
            fields='id, name'
        ).execute()
        return updated_file
    except Exception as e:
        st.error(f"Error renaming file: {e}")
        return None