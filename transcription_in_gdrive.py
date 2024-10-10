#Open AI API Key is pulling from terminal command: export OPENAI_API_KEY = 'apikey'
import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build
from openai import OpenAI
from dotenv import load_dotenv
from datetime import datetime
import os
import io
import ffmpeg #had to brew install
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# Define scopes and load credentials
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/documents']
creds = service_account.Credentials.from_service_account_file('credentials.json', scopes=SCOPES)
OpenAI.api_key = st.secrets["OPENAI_API_KEY"]

# Initialize the Drive API client
drive_service = build('drive', 'v3', credentials=creds)
docs_service = build('docs', 'v1', credentials=creds)
client = OpenAI()

# Define folder IDs
unprocessed_audio_folder_id = '10asUMD9jFbWlIXsTxqSezPdJkJU8czdm'
transcribed_audio_folder_id = '1KfdDf2LR7abUn-TpG9MrjYv3fhGXHmox'
transcribed_text_folder_id = '1HVT-YrVNnMy4ag0h6hqawl2PVef-Fc0C'
trash_folder_id = '1TZzr1cxQGxohvFRR63kip7PxMCWExwTR'

# Function to list .mp3 or .mpg files in a folder
def list_audio_files(folder_id):
    query = f"'{folder_id}' in parents and (mimeType contains 'audio/' or mimeType contains 'video/')"
    results = drive_service.files().list(q=query, fields="files(id, name, mimeType)").execute()
    files = results.get('files', [])
    # Debugging: Print the file types found
    for file in files:
        print(f"Found file: {file['name']} with MIME type: {file['mimeType']}")
    return files

# Function to move the file to another folder (trash folder)
def move_file_to_trash(file_id, trash_folder_id):
    try:
        # Retrieve the existing parents (folders) of the file
        file = drive_service.files().get(fileId=file_id, fields='parents').execute()
        previous_parents = ",".join(file.get('parents'))

        # Debugging: Print file ID and parent folder info
        print(f"Moving file {file_id} from parents {previous_parents} to trash folder {trash_folder_id}")

        # Attempt to move the file to the trash folder
        updated_file = drive_service.files().update(
            fileId=file_id,
            addParents=trash_folder_id,
            removeParents=previous_parents,
            fields='id, parents'
        ).execute()

        print(f"File ID {file_id} moved to trash folder ID {trash_folder_id}")
    except Exception as e:
        print(f"Error moving file {file_id}: {str(e)}")
        raise  # Re-raise the exception for further debugging


# Function to convert .mpg to .mp3 using ffmpeg
def convert_mpg_to_mp3(mpg_file_path, mp3_file_path):
    try:
        # Debugging: Print conversion details
        print(f"Converting {mpg_file_path} to {mp3_file_path}")
        ffmpeg.input(mpg_file_path).output(mp3_file_path).run()
        print(f"Converted {mpg_file_path} to {mp3_file_path}")
        return mp3_file_path
    except ffmpeg.Error as e:
        print(f"Error converting file: {e}")
        return None

# Function to download a file from Google Drive
def download_file(file_id, file_name):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_name, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print(f"Download {file_name}: {int(status.progress() * 100)}%.")
    return file_name

# Function to upload a file to Google Drive
def upload_file_to_drive(file_path, folder_id, mime_type='audio/mpeg'):
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
    print(f"Uploaded file {file_path} to Google Drive with ID: {uploaded_file.get('id')}")
    return uploaded_file.get('id')

# Function to generate the new filename with the timestamp
def generate_transcribed_filename():
    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    return f"signal-{timestamp}.mp3"

# Transcribe audio using OpenAI Whisper
def transcribe(audio_file_path):
    with open(audio_file_path, 'rb') as audio_file:
        transcription = client.audio.transcriptions.create(
            model="whisper-1", 
            file=audio_file,
        )
    return transcription.text

# Create a Google Doc with the transcription content
def create_google_doc(service, title, content):
    doc_body = {'title': title}
    doc = service.documents().create(body=doc_body).execute()
    document_id = doc.get('documentId')

    # Update the document with the transcription content
    requests = [{
        'insertText': {
           'location': {'index': 1},
            'text': content
        }
    }]
    service.documents().batchUpdate(documentId=document_id, body={'requests': requests}).execute()

    print(f'Document created with ID: {document_id}')
    return document_id

# Move file to a different Google Drive folder
def move_file_to_folder(file_id, target_folder_id):
    try:
        # Retrieve the existing parents to remove
        file = drive_service.files().get(fileId=file_id, fields='parents').execute()
        previous_parents = ",".join(file.get('parents'))

        # Move the file to the new folder
        updated_file = drive_service.files().update(
            fileId=file_id,
            addParents=target_folder_id,
            removeParents=previous_parents,
            fields='id, parents'
        ).execute()

        print(f"File ID {file_id} moved to folder ID {target_folder_id}")
    except Exception as e:
        print(f"Error moving file {file_id}: {str(e)}")

# Function to create a shareable link for a file
def get_shareable_link(file_id):
    try:
        # Update file permissions to make it shareable (optional: adjust to 'anyoneWithLink' for broader access)
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

# Streamlit UI
st.image("Echelon_Icon_Sky Blue.png", caption="The Home for Aliens", width = 125)
st.title("NOS Daily Digest Transcription App - Custom Built for Kerri Faber")
st.write("Once you have uploaded your files to the folder linked below, click the 'Transcribe Audio Files' button to transcribe. Full instructions are available on Notion.")
st.markdown('[Upload Folder](https://drive.google.com/drive/folders/10asUMD9jFbWlIXsTxqSezPdJkJU8czdm?usp=drive_link)')
st.markdown('[Notion](https://www.notion.so/Pulse-4799295f90594380b55f75e0d78dbb03?p=11b9668a26d680e39d57e8243d8f7178&pm=s)')

# Add a reset button
if st.button('Reset App'):
    st.query_params.clear()  # Simulate a reset by clearing query parameters

import os

if st.button('Transcribe Audio Files'):
    st.write("Transcription started...")

    try:
        # Step 1: List all .mp3 and .mpg files in the unprocessed audio folder
        audio_files = list_audio_files(unprocessed_audio_folder_id)
        count = 0
        for file in audio_files:
            file_id = file['id']
            file_name = file['name']  # Original file name
            mime_type = file['mimeType']
            count += 1

            # Step 2: Download the original file (before any conversion)
            original_audio_path = download_file(file_id, file_name)
            st.write(f"Downloaded file: {file_name} with MIME type: {mime_type}")

            final_audio_path = original_audio_path  # Initially set to the original file
            final_file_name = file_name  # Keep track of the final file name for the document title

            # Step 3: Check if it's an .mpg file and convert it to .mp3
            if 'video' in mime_type:
                converted_file_name = generate_transcribed_filename()  # Generate a name for the converted file
                converted_mp3_path = convert_mpg_to_mp3(original_audio_path, converted_file_name)

                if converted_mp3_path:
                    final_audio_path = converted_mp3_path  # Now use the converted file for further processing
                    final_file_name = converted_file_name  # Update the final file name for the document title
                    st.write(f"Converted {file_name} to .mp3 format for transcription.")

            # Step 4: Transcribe the audio
            transcription_text = transcribe(final_audio_path)
            st.write(f"Transcription for {final_file_name}: {transcription_text}")

            # Step 5: Upload the .mp3 file (converted or original) to Google Drive
            mp3_file_id = upload_file_to_drive(final_audio_path, transcribed_audio_folder_id)
            st.write(f".mp3 file uploaded to Google Drive with ID: {mp3_file_id}")

            # Step 6: Move the original file to trash folder
            move_file_to_trash(file_id, trash_folder_id)
            st.write(f"Moved {file_name} to trash folder.")

            # Step 7: Add date and "Who Recorded This Audio?" before the transcription
            timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
            date_transcribed = f"Date Transcribed: {timestamp}"
            who_recorded = "Who Recorded This Audio? Kerri Faber / Erik Allen / David McColl / Joel Moxley / Christian Bader"

            final_transcription_text = f"{date_transcribed}\n\n{who_recorded}\n\n{transcription_text}\n\n"

            # Step 8: Get the shareable link for the mp3 file
            shareable_link = get_shareable_link(mp3_file_id)

            # Step 9: Create a Google Doc for the transcription
            # Use the final file name (either original or converted) for the document title
            doc_title = f"{final_file_name}_INITIALS_TRANSCRIPTION_FOR REVIEW"
            doc_body = {'title': doc_title}
            doc = docs_service.documents().create(body=doc_body).execute()
            document_id = doc.get('documentId')

            # Insert transcription text first
            requests = [
                {
                    'insertText': {
                        'location': {'index': 1},
                        'text': final_transcription_text
                    }
                }
            ]
            docs_service.documents().batchUpdate(documentId=document_id, body={'requests': requests}).execute()

            # After inserting text, calculate the index for hyperlink insertion
            start_index = len(final_transcription_text) + 1  # start after the transcription text
            link_text = f"\nMP3 File: {shareable_link}"

            # Insert the MP3 link and make it clickable
            requests = [
                {
                    'insertText': {
                        'location': {'index': start_index},
                        'text': link_text
                    }
                },
                {
                    'updateTextStyle': {
                        'range': {
                            'startIndex': start_index,
                            'endIndex': start_index + len(link_text)
                        },
                        'textStyle': {
                            'link': {
                                'url': shareable_link
                            }
                        },
                        'fields': 'link'
                    }
                }
            ]

            docs_service.documents().batchUpdate(documentId=document_id, body={'requests': requests}).execute()
            st.write(f"Google Doc created with destination: https://docs.google.com/document/d/{document_id}")

            # Step 10: Move the Google Doc to the transcribed text folder
            move_file_to_folder(document_id, transcribed_text_folder_id)

            # Step 11: Clean up the local files after all processing
            # Delete the original .mpg file if it exists
            if 'video' in mime_type and os.path.exists(original_audio_path):
                os.remove(original_audio_path)
                st.write(f"Deleted original .mpg file: {original_audio_path}")

            # Delete the converted .mp3 file after uploading and transcription
            if os.path.exists(final_audio_path):
                os.remove(final_audio_path)
                st.write(f"Deleted local .mp3 file: {final_audio_path}")

    except Exception as e:
        st.error(f"Error during transcription: {str(e)}")

    st.success(f"{count} transcription(s) complete! Find files in the folder linked below.")
    st.markdown('[Transcriptions Folder](https://drive.google.com/drive/u/0/folders/1HVT-YrVNnMy4ag0h6hqawl2PVef-Fc0C)')
