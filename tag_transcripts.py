# Streamlit App: Enhanced Transcript Tagging with Corrected Note Creation and Associations

# ------------------------------------------------------------
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# HubSpot Entity Tagging Step for Echelon NOS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Authors: Christian Bader
# October 2024 - Present
#
# This script takes a transcription as input and allows a user to tag entities
# mentioned in the transcript. Tagged entities are directly linked on HubSpot.
# Any tagged entity that does not already exist in HubSpot will have a new unique
# profile created, then will be tagged. "Tagging" links the Google Doc with the
# transcript to the relevant customer/company profile in HubSpot with a Note.
# The Google Doc's metadata store will be updated to include the linked Hubspot IDs.
#
# Deployed on Streamlit Cloud.
# ------------------------------------------------------------

# ------------------------------
# Import Statements
# ------------------------------
import json
import logging
import re
import io
import os
from datetime import datetime

import streamlit as st
import requests

from google.oauth2 import service_account
from googleapiclient.discovery import build

import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import base64

# ------------------------------
# Configuration and Initialization
# ------------------------------

# Set Streamlit page configuration
st.set_page_config(
    page_title="NOS | Tag Transcripts",
    page_icon="Echelon_Icon_Sky Blue.png",
    layout="wide"
)

# Display Echelon logo
st.image("Echelon_Icon_Sky Blue.png", caption="The Home for Aliens", width=125)
st.title("NOS - Tag Transcripts")
st.write("Custom Built for Kerri Faber")

# ------------------------------
# Define HubSpot Credentials and Headers
# ------------------------------
HUBSPOT_API_TOKEN = st.secrets["hubspot"]["api_token"]
hubspot_portal_id = st.secrets["hubspot"]["portal_id"]  # Initialize Portal ID from secrets

headers = {
    "Authorization": f"Bearer {HUBSPOT_API_TOKEN}",
    "Content-Type": "application/json"
}

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------
# Define Google API Scopes and Initialize Clients
# ------------------------------
SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/documents',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/gmail.send'  # Added Gmail scope
]
gcp_secrets = st.secrets["gcp_service_account"]
creds = service_account.Credentials.from_service_account_info(
    gcp_secrets,
    scopes=SCOPES
)
drive_service = build('drive', 'v3', credentials=creds)
docs_service = build('docs', 'v1', credentials=creds)
sheets_service = build('sheets', 'v4', credentials=creds)

# ------------------------------
# Define Google Drive Folder and Spreadsheet IDs
# ------------------------------
# PRODUCTION
GD_FOLDER_ID_TRANSCRIBED_TEXT = st.secrets["gdrive"]["GD_FOLDER_ID_TRANSCRIBED_TEXT_PROD"]
GD_FOLDER_ID_TAGGED_TEXT = st.secrets["gdrive"]["GD_FOLDER_ID_TAGGED_TEXT_PROD"]
GD_SPREADSHEET_ID_INGRESS_LOG = st.secrets["gdrive"]["GD_SPREADSHEET_ID_INGRESS_LOG_PROD"]
GD_SHEET_NAME_INGRESS_LOG = 'tag_transcripts'

# TESTING
# GD_FOLDER_ID_TRANSCRIBED_TEXT = st.secrets["gdrive"]["GD_FOLDER_ID_TRANSCRIBED_TEXT_TEST"]
# GD_FOLDER_ID_TAGGED_TEXT = st.secrets["gdrive"]["GD_FOLDER_ID_TAGGED_TEXT_TEST"]
# GD_SPREADSHEET_ID_INGRESS_LOG = st.secrets["gdrive"]["GD_SPREADSHEET_ID_INGRESS_LOG_TEST"]
# GD_SHEET_NAME_INGRESS_LOG = 'tag_transcripts'

# ------------------------------
# Define Google Drive and HubSpot Functions
# ------------------------------

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
        st.error(f"Error moving file {file_id}: {str(e)}")

def gd_extract_file_id(drive_link):
    """
    Extracts the file ID from a Google Drive or Google Docs link.

    Parameters:
        drive_link (str): The raw URL.

    Returns:
        str: The Google Drive file ID.
    """
    # Regular expressions to extract the file ID from different Google URLs
    patterns = [
        r'https://drive\.google\.com/file/d/([a-zA-Z0-9_-]+)',
        r'https://docs\.google\.com/document/d/([a-zA-Z0-9_-]+)',
        r'https://drive\.google\.com/open\?id=([a-zA-Z0-9_-]+)'
    ]
    for pattern in patterns:
        match = re.search(pattern, drive_link)
        if match:
            return match.group(1)
    else:
        st.error("Invalid Google Drive or Google Docs link.")
        return None

def gd_get_file_properties(file_id):
    """
    Retrieves the properties of a file from Google Drive.

    Parameters:
        file_id (str): The ID of the file.

    Returns:
        dict: A dictionary containing the file's properties.
    """
    try:
        file = drive_service.files().get(fileId=file_id, fields='properties').execute()
        properties = file.get('properties', {})
        return properties
    except Exception as e:
        st.error(f"Error fetching file properties: {e}")
        return {}

def gd_update_file_properties(file_id, new_properties):
    """
    Clears all existing properties of a file in Google Drive and sets new properties.

    Parameters:
        file_id (str): The ID of the file.
        new_properties (dict): A dictionary of new properties to set.

    Returns:
        dict: The updated file resource.
    """
    try:
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
    except Exception as e:
        st.error(f"Error updating file properties: {e}")
        return {}

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

def get_all_companies():
    """
    Retrieves all companies from the HubSpot CRM and returns them as a list of dictionaries.
    """
    all_companies = []
    after = None
    url_companies = "https://api.hubapi.com/crm/v3/objects/companies"
    while True:
        params = {'limit': 100, 'properties': 'name'}
        if after:
            params['after'] = after
        try:
            response = requests.get(url_companies, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            all_companies.extend(data.get('results', []))
            paging = data.get('paging')
            if paging and 'next' in paging:
                after = paging['next']['after']
            else:
                break
        except requests.exceptions.RequestException as e:
            st.error(f"An error occurred while fetching companies: {e}")
            break
    return all_companies

def get_all_contacts():
    """
    Retrieves all contacts from the HubSpot CRM and returns them as a list of dictionaries.
    """
    all_contacts = []
    after = None
    url_contacts = "https://api.hubapi.com/crm/v3/objects/contacts"
    while True:
        params = {'limit': 100, 'properties': 'firstname,lastname,email'}
        if after:
            params['after'] = after
        try:
            response = requests.get(url_contacts, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            all_contacts.extend(data.get('results', []))
            paging = data.get('paging')
            if paging and 'next' in paging:
                after = paging['next']['after']
            else:
                break
        except requests.exceptions.RequestException as e:
            st.error(f"An error occurred while fetching contacts: {e}")
            break
    return all_contacts

def create_note(note_body, hs_timestamp):
    """
    Creates a Note in HubSpot with the given body content and timestamp.

    Parameters:
        note_body (str): The body content of the note.
        hs_timestamp (int): The timestamp of the note in milliseconds since the Unix epoch.

    Returns:
        str: The ID of the created note, or None if creation failed.
    """

    url = "https://api.hubapi.com/crm/v3/objects/notes"
    data = {
        "properties": {
            "hs_note_body": note_body,  # 'hs_note_body' is the property for the note content
            "hs_timestamp": hs_timestamp
        }
    }
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        note = response.json()
        note_id = note.get('id')
        return note_id
    except requests.exceptions.HTTPError as e:
        st.error(f"An error occurred while creating the note: {e}")
        st.error(f"Response content: {e.response.text}")
        return None
    except Exception as e:
        st.error(f"An unexpected error occurred while creating the note: {e}")
        return None

def associate_note_with_objects(note_id, company_ids, contact_ids):
    """
    Associates the created Note with specified companies and contacts.

    Parameters:
        note_id (str): The ID of the created note.
        company_ids (list): List of company IDs to associate.
        contact_ids (list): List of contact IDs to associate.

    Returns:
        bool: True if associations were successful, False otherwise.
    """
    association_types = {
        'companies': 'note_to_company',
        'contacts': 'note_to_contact'
    }

    success = True

    for company_id in company_ids:
        url = f"https://api.hubapi.com/crm/v3/objects/notes/{note_id}/associations/companies/{company_id}/{association_types['companies']}"
        try:
            response = requests.put(url, headers=headers)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            st.error(f"Error associating company ID {company_id} with note: {e}")
            st.error(f"Response content: {e.response.text}")
            success = False
        except Exception as e:
            st.error(f"Unexpected error while associating company ID {company_id}: {e}")
            success = False

    for contact_id in contact_ids:
        url = f"https://api.hubapi.com/crm/v3/objects/notes/{note_id}/associations/contacts/{contact_id}/{association_types['contacts']}"
        try:
            response = requests.put(url, headers=headers)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            st.error(f"Error associating contact ID {contact_id} with note: {e}")
            st.error(f"Response content: {e.response.text}")
            success = False
        except Exception as e:
            st.error(f"Unexpected error while associating contact ID {contact_id}: {e}")
            success = False

    return success

def create_company(name):
    """
    Creates a new company in HubSpot with the given name.
    """
    url = "https://api.hubapi.com/crm/v3/objects/companies"
    data = {
        "properties": {
            "name": name
        }
    }
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        st.error(f"An error occurred while creating the company: {e}")
        st.error(f"Response content: {e.response.text}")
        return None
    except Exception as e:
        st.error(f"An unexpected error occurred while creating the company: {e}")
        return None

def create_contact(firstname, lastname, email=None):
    """
    Creates a new contact in HubSpot with the given details.
    """
    url = "https://api.hubapi.com/crm/v3/objects/contacts"
    data = {
        "properties": {
            "firstname": firstname,
            "lastname": lastname,
        }
    }
    if email:
        data["properties"]["email"] = email
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        st.error(f"An error occurred while creating the contact: {e}")
        st.error(f"Response content: {e.response.text}")
        return None
    except Exception as e:
        st.error(f"An unexpected error occurred while creating the contact: {e}")
        return None

def get_contact_by_id(contact_id):
    """
    Retrieves a contact's full name by ID from HubSpot.
    """
    url = f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}"
    params = {'properties': 'firstname,lastname'}
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        firstname = data.get('properties', {}).get('firstname', '')
        lastname = data.get('properties', {}).get('lastname', '')
        full_name = f"{firstname} {lastname}".strip()
        return full_name
    except Exception as e:
        st.error(f"Error fetching contact by ID: {e}")
        return "Unknown Contact"

def get_company_by_id(company_id):
    """
    Retrieves a company's name by ID from HubSpot.
    """
    url = f"https://api.hubapi.com/crm/v3/objects/companies/{company_id}"
    params = {'properties': 'name'}
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        name = data.get('properties', {}).get('name', '')
        return name
    except Exception as e:
        st.error(f"Error fetching company by ID: {e}")
        return "Unknown Company"

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

# ------------------------------
# Main Streamlit Application
# ------------------------------

# Text input to accept a Google Drive or Google Docs link
drive_link = st.text_input('Enter the Google Drive or Google Docs link to the document')
raw_transcripts_link = gd_get_shareable_link(GD_FOLDER_ID_TRANSCRIBED_TEXT)
st.markdown(f'[Raw Transcripts Google Drive Folder]({raw_transcripts_link})')

# Check if the link has been provided
if drive_link:
    # Extract the file ID from the provided link
    gd_transcript_file_id = gd_extract_file_id(drive_link)
    if gd_transcript_file_id:
        gd_transcript_file_properties = gd_get_file_properties(gd_transcript_file_id)
        datetime_transcribed = gd_transcript_file_properties.get('transcription_timestamp')
        datetime_uploaded = gd_transcript_file_properties.get('upload_timestamp')
        seconds_transcribed = gd_transcript_file_properties.get('duration_seconds')
        gd_input_audio_file_link = gd_transcript_file_properties.get('raw_audio_file_link')
        gd_output_mp3_file_link = gd_transcript_file_properties.get('mp3_file_link')

        # Display success message if the link is valid
        st.success("Google Drive link is valid.")

        # --- Fetch Companies and Contacts ---
        # Check if companies data is already stored in session state
        if 'companies_data' not in st.session_state:
            # Show a spinner while fetching companies data
            with st.spinner('Fetching companies...'):
                st.session_state['companies_data'] = get_all_companies()

        # Check if contacts data is already stored in session state
        if 'contacts_data' not in st.session_state:
            # Show a spinner while fetching contacts data
            with st.spinner('Fetching contacts...'):
                st.session_state['contacts_data'] = get_all_contacts()

        # Retrieve companies and contacts data from session state
        companies_data = st.session_state['companies_data']
        contacts_data = st.session_state['contacts_data']

        # Create a dictionary for companies with name as the key and ID as the value
        company_options = {
            f"{company.get('properties', {}).get('name', 'Unnamed Company')} [{company.get('id')}]": company.get('id')
            for company in companies_data
        }

        # Create a dictionary for contacts with "firstname lastname [ID]" as the key and ID as the value
        contact_options = {
            f"{contact.get('properties', {}).get('firstname', '')} {contact.get('properties', {}).get('lastname', '')} [{contact.get('id')}]": contact.get('id')
            for contact in contacts_data
        }

        # Provide a disclaimer for duplicate names
        st.write("**Note:** If there are duplicate names in the selection lists, please refer to the contact ID in brackets to verify the correct contact in HubSpot.")

        # Text input for one-line title snippet
        transcript_title = st.text_area('Provide a title for this transcript. Keep it short!')

        # Multiselect for selecting contact who recorded the message
        who_recorded = st.multiselect(
            'Who recorded this? Only select one name.',
            options=list(contact_options.keys()),
            max_selections=1  # Ensure only one selection
        )

        # --- Notes ---
        # Text area for entering notes to be added to the engagement
        action_items = st.text_area('Enter your action items here. Be specific!')

        # Clean the action_items to ensure it's a single line
        action_items_single_line = re.sub(r'\s+', ' ', action_items).strip()

        # Multiselect for selecting companies to tag in the engagement
        selected_companies = st.multiselect(
            'Tag Companies (already in HubSpot)',
            options=list(company_options.keys())
        )

        # Multiselect for selecting contacts to tag in the engagement
        selected_contacts = st.multiselect(
            'Tag Contacts (already in HubSpot)',
            options=list(contact_options.keys())
        )

        # Input for creating new companies to tag in the engagement
        st.header("Add New Companies to HubSpot")
        st.write("**Please enter one company name per line.**")
        new_companies_input = st.text_area('Enter names of companies to create in HubSpot')

        # Input for creating new contacts to tag in the engagement
        st.header("Add New Contacts to HubSpot")
        st.write("**Please enter contacts in the format 'First Middle Last', one per line. If the contact has multiple first names or middle names, include them before the last name. The last word will be treated as the last name.**")
        new_contacts_input = st.text_area('Enter names of contacts to create in HubSpot')

        if st.button('Submit'):
            # Handle the submission process

            # Initialize lists
            contacts_created_formatted = []
            companies_created_formatted = []
            new_company_ids = []
            new_company_names = []
            new_contact_ids = []
            new_contact_names = []
            company_ids = []
            contact_ids = []
            recorder_contact_ids = []

            # Create new HubSpot companies
            if new_companies_input.strip():
                new_company_names = [name.strip() for name in new_companies_input.strip().split('\n') if name.strip()]
                for company_name in new_company_names:
                    # Check if the company already exists (to avoid duplicates)
                    existing_companies = [key for key in company_options.keys() if key.startswith(company_name)]
                    if not existing_companies:
                        st.info(f"Creating new company: {company_name}")
                        company_response = create_company(company_name)
                        if company_response and 'id' in company_response:
                            company_id = company_response['id']
                            new_company_ids.append(company_id)
                            # Update the company_options dictionary
                            company_options[f"{company_name} [{company_id}]"] = company_id
                            # Append to companies_created_formatted
                            companies_created_formatted.append(f"{company_name} [{company_id}]")
                        else:
                            st.error(f"Failed to create company: {company_name}")
                    else:
                        st.warning(f"Company '{company_name}' already exists in HubSpot.")
                        company_id = company_options[existing_companies[0]]
                        new_company_ids.append(company_id)
                        # Append to companies_created_formatted (even if it exists)
                        companies_created_formatted.append(f"{company_name} [{company_id}]")
            else:
                new_company_names = []

            # Create new HubSpot contacts
            if new_contacts_input.strip():
                new_contact_names = [name.strip() for name in new_contacts_input.strip().split('\n') if name.strip()]
                for contact_name in new_contact_names:
                    # Normalize whitespace within the name
                    contact_name = ' '.join(contact_name.split())
                    # Split the name into parts
                    names = contact_name.split()
                    if len(names) >= 2:
                        # Assign all but the last word to the first name
                        firstname = ' '.join(names[:-1])
                        # The last word is the last name
                        lastname = names[-1]
                        full_name = f"{firstname} {lastname}"
                        # Check for existing contacts with the same name
                        existing_contacts = [key for key in contact_options.keys() if key.startswith(full_name)]
                        if not existing_contacts:
                            st.info(f"Creating new contact: {full_name}")
                            contact_response = create_contact(firstname, lastname)
                            if contact_response and 'id' in contact_response:
                                contact_id = contact_response['id']
                                new_contact_ids.append(contact_id)
                                # Update the contact_options dictionary
                                contact_options[f"{full_name} [{contact_id}]"] = contact_id
                                # Append to contacts_created_formatted
                                contacts_created_formatted.append(f"{full_name} [{contact_id}]")
                            else:
                                st.error(f"Failed to create contact: {full_name}")
                        else:
                            st.warning(f"Contact '{full_name}' already exists in HubSpot.")
                            contact_id = contact_options[existing_contacts[0]]
                            new_contact_ids.append(contact_id)
                            # Append to contacts_created_formatted (even if it exists)
                            contacts_created_formatted.append(f"{full_name} [{contact_id}]")
                    else:
                        st.error(f"Invalid contact name format: '{contact_name}'. Each contact must include at least a first name and a last name, separated by spaces.")
            else:
                new_contact_names = []

            # Map selected company names to their corresponding IDs
            company_ids = [company_options[name] for name in selected_companies]
            # Map selected contact names to their corresponding IDs
            contact_ids = [contact_options[name] for name in selected_contacts]

            # Map selected recorder names to their corresponding IDs
            recorder_contact_ids = [contact_options[name] for name in who_recorded if name in contact_options]
            # Add the recorder's contact IDs to the list of contact IDs
            contact_ids.extend(recorder_contact_ids)

            # Add the new company and contact IDs
            company_ids.extend(new_company_ids)
            contact_ids.extend(new_contact_ids)

            # Remove duplicates
            company_ids = list(set(company_ids))
            contact_ids = list(set(contact_ids))

            # --- SHEETS LOG ---
            # Get the current datetime for datetime_tagged in the desired format
            datetime_tagged = datetime.now().strftime('%Y-%m-%d-%H%M%S%f')  # Example: 2024-10-15-163816317000

            # Format who_recorded
            who_recorded_formatted = who_recorded[0] if who_recorded else ''

            # Prepare contacts_linked_formatted
            contacts_linked_formatted = selected_contacts.copy()

            # Ensure who_recorded is included in contacts_linked_formatted
            if who_recorded_formatted and who_recorded_formatted not in contacts_linked_formatted:
                contacts_linked_formatted.append(who_recorded_formatted)

            # Remove duplicates
            contacts_linked_formatted = list(set(contacts_linked_formatted))

            # Prepare companies_linked_formatted
            companies_linked_formatted = selected_companies.copy()
            # Remove duplicates
            companies_linked_formatted = list(set(companies_linked_formatted))

            # Ensure that contacts_created_formatted and companies_created_formatted are defined
            contacts_created_formatted = contacts_created_formatted if contacts_created_formatted else []
            companies_created_formatted = companies_created_formatted if companies_created_formatted else []

            # Prepare the row data
            row = [
                gd_transcript_file_id,                  # Column A: File ID
                datetime_tagged,                        # Column B: Datetime Tagged
                transcript_title,                       # Column C: Transcript Title
                who_recorded_formatted,                 # Column D: Who Recorded
                action_items,                           # Column E: Action Items
                ', '.join(contacts_linked_formatted),   # Column F: Contacts Linked
                ', '.join(companies_linked_formatted),  # Column G: Companies Linked
                ', '.join(contacts_created_formatted),  # Column H: Contacts Created
                ', '.join(companies_created_formatted), # Column I: Companies Created
                '0',                                    # Column J: Merge Status Tag
                len(contacts_linked_formatted),         # Column K: # Contacts Linked
                len(companies_linked_formatted),        # Column L: # Companies Linked
                len(contacts_created_formatted),        # Column M: # Contacts Created
                len(companies_created_formatted)        # Column N: # Companies Created
            ]

            try:
                # Append the row to the spreadsheet
                request = sheets_service.spreadsheets().values().append(
                    spreadsheetId=GD_SPREADSHEET_ID_INGRESS_LOG,
                    range=f'{GD_SHEET_NAME_INGRESS_LOG}!A:J',  # Include column J
                    valueInputOption='RAW',
                    insertDataOption='INSERT_ROWS',
                    body={'values': [row]}
                )
                response = request.execute()
                st.success("Logged data to the spreadsheet.")
            except Exception as e:
                st.error(f"Error writing to spreadsheet: {str(e)}")

            # --- METADATA WRITE ---
            new_properties = {
                'datetime_uploaded': datetime_uploaded,
                'datetime_transcribed': datetime_transcribed,
                'datetime_tagged': datetime_tagged,
                'seconds_transcribed': str(seconds_transcribed),
                'gd_input_audio_file_link': gd_input_audio_file_link,
                'gd_output_mp3_file_link': gd_output_mp3_file_link,
                'who_recorded_ids': who_recorded_formatted,
                'file_title': transcript_title,
            }

            gd_update_file_properties(gd_transcript_file_id, new_properties)
            test_metadata = gd_get_file_properties(gd_transcript_file_id)
            st.success(f"File metadata updated.")
            st.write(f"Metadata: {test_metadata}")

            # Rename file and move to processed gd folder
            if who_recorded:
                recorder_name = who_recorded[0].split(' [')[0].upper()
                new_file_name = f"SIGNAL_{datetime_uploaded}_{recorder_name}_{transcript_title.upper()}_TRANSCRIPT__TAGGED.docx"
                gd_rename_file(gd_transcript_file_id, new_file_name)

            gd_move_file_between_folders(gd_transcript_file_id, GD_FOLDER_ID_TAGGED_TEXT)
            st.success(f"File moved to processed folder.")
            st.write(f"Folder ID: {GD_FOLDER_ID_TAGGED_TEXT}")

            # --- HUBSPOT DATA WRITE ---
            action_items_html = action_items.replace('\n','<br>')
            note_body = f"This entity was tagged in a transcript: <a href=\"{drive_link}\">{transcript_title}</a><br>Recorded by: {who_recorded_formatted}<br>Action Items: <br>{action_items_html}"

            # Calculate hs_timestamp (convert datetime_tagged to milliseconds since epoch)
            datetime_tagged_obj = datetime.strptime(datetime_tagged, '%Y-%m-%d-%H%M%S%f')
            hs_timestamp = int(datetime_tagged_obj.timestamp() * 1000)
            # Create the note
            with st.spinner('Creating note in HubSpot...'):
                note_id = create_note(note_body, hs_timestamp)

            if note_id:
                st.success("Note created successfully.")

                # Associate the note with companies and contacts
                with st.spinner('Associating note with companies and contacts...'):
                    association_success = associate_note_with_objects(note_id, company_ids, contact_ids)

                if association_success:
                    st.success("Note associated with companies and contacts successfully.")
                else:
                    st.error("Failed to associate note with some companies or contacts.")
            else:
                st.error("Failed to create note.")

            # --- Logging to Session State ---
            transcription_entry = {
                'gd_transcript_file_id': gd_transcript_file_id,
                'datetime_tagged': datetime_tagged,
                'transcript_title': transcript_title,
                'who_recorded': who_recorded_formatted,
                'action_items': action_items_single_line,
                'contacts_linked': contacts_linked_formatted,
                'companies_linked': companies_linked_formatted,
                'contacts_created': contacts_created_formatted,
                'companies_created': companies_created_formatted
            }

            st.success("Transcription processed and logged successfully.")
            st.write("To submit another transcript, refresh the page and repeat this process with a new file.")

            st.stop()  # Prevent further interaction

# ------------------------------
# Additional Notes
# ------------------------------
st.markdown("---")  # Add a separator at the bottom
st.write("© 2024 Echelon NOS. All rights reserved.")