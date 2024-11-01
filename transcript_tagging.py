"""
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
HubSpot Entity Tagging Step for Echelon NOS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Authors: Christian Bader
October 2024 - Present

This script takes a transcription as input and allows a user to tag entities
mentioned in the transcript. Tagged entities are directly linked on HubSpot.
Any tagged entity that does not already exist in HubSpot will have a new unique
profile created, then will be tagged. "Tagging" links the Google Doc with the
transcript to the relevant customer/company profile in HubSpot with a Note.
The Google Doc's metadata store will be updated to include the linked Hubspot IDs.
For example: 
Metadata Example:
    {
    'file_title': 'Test', 
    'action_items': 'Remind me to do this'
    'transcription_timestamp': '2024-10-31-164400836774',
    'audio_file_link': 'https://drive.google.com/file/d/1wA1eO5L2nd8MK_0U_y-uise8NRvXB1PO/view?usp=drivesdk',  
    'duration_seconds': '16.347', 
    'who_recorded_ids': '58577394199', 
    'new_contact_ids': '74371999087', 
    'new_company_ids': '25402526277'
    'contacts_linked_ids': '58577394199,101,74371999087', , 
    'companies_linked_ids': '19198305190,25402526277', 
    }
"""

# Standard Python library imports
import json
import re
import io
import os

# Open source imports
import streamlit as st
import requests

# API imports
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Define HubSpot credentials, initialize client
HUBSPOT_API_TOKEN = st.secrets["hubspot_api_token"]
headers = {
    "Authorization": f"Bearer {HUBSPOT_API_TOKEN}",
    "Content-Type": "application/json"
}

# Define Google scopes/credentials, initialize client
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/documents']
creds = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=SCOPES
)
drive_service = build('drive', 'v3', credentials=creds)
docs_service = build('docs', 'v1', credentials=creds)

# Define Google Drive folder IDs

# PRODUCTION
# TRANSCRIBED_TEXT_GD_FOLDER_ID = '1HVT-YrVNnMy4ag0h6hqawl2PVef-Fc0C'
# TAGGED_TEXT_GD_FOLDER_ID = '1WhBzd0ehQQgAWvlG_J0KBefICe6r2ceA'

# TESTING
TRANSCRIBED_TEXT_GD_FOLDER_ID = '1joWp7fS4XeHYSF-T3FrxiHu4gMTBzcw4'
TAGGED_TEXT_GD_FOLDER_ID = '150bxcdT0h9gkeDrGZRgpBelPK2prRps7'


# Define functions that leverage Google Drive API

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

# Define functions that leverage HubSpot API

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

def create_engagement(note_body, company_ids, contact_ids):
    """
    Creates an engagement of type 'NOTE' in HubSpot and associates it with specified companies and contacts.
    """
    url = "https://api.hubapi.com/engagements/v1/engagements"
    data = {
        "engagement": {
            "active": True,
            "type": "NOTE",
        },
        "associations": {
            "companyIds": company_ids,
            "contactIds": contact_ids,
        },
        "metadata": {
            "body": note_body,
        }
    }
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        st.error(f"An error occurred while creating the engagement: {e}")
        st.error(f"Response content: {e.response.text}")
        return None
    except Exception as e:
        st.error(f"An unexpected error occurred while creating the engagement: {e}")
        return None

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
    url = f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}"
    params = {'properties': 'firstname,lastname'}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    firstname = data.get('properties', {}).get('firstname', '')
    lastname = data.get('properties', {}).get('lastname', '')
    full_name = f"{firstname} {lastname}".strip()
    return full_name

def get_company_by_id(company_id):
    url = f"https://api.hubapi.com/crm/v3/objects/companies/{company_id}"
    params = {'properties': 'name'}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    name = data.get('properties', {}).get('name', '')
    return name

# Example of Retrieving Names from Stored IDs
# # Retrieve file properties
# file_properties = gd_get_file_properties(file_id)

# # Get IDs from properties
# contact_ids_str = file_properties.get('contacts_linked_ids', '')
# company_ids_str = file_properties.get('companies_linked_ids', '')

# # Convert strings back to lists
# contact_ids = contact_ids_str.split(',') if contact_ids_str else []
# company_ids = company_ids_str.split(',') if company_ids_str else []

# # Fetch contact names
# contact_names = [get_contact_by_id(contact_id) for contact_id in contact_ids]

# # Fetch company names
# company_names = [get_company_by_id(company_id) for company_id in company_ids]

# # Now you have lists of names
# print("Contacts Linked:", contact_names)
# print("Companies Linked:", company_names)

# --- Streamlit App ---

# Set the title of the Streamlit app
st.title("NOS - Tag Transcripts")

# Text input to accept a Google Drive or Google Docs link
drive_link = st.text_input('Enter the Google Drive or Google Docs link to the document')

# Check if the link has been provided
if drive_link:
    # Extract the file ID from the provided link
    file_id = gd_extract_file_id(drive_link)
    if file_id:
        file_properties = gd_get_file_properties(file_id)
        transcription_timestamp = file_properties.get('transcription_timestamp')
        duration_seconds = file_properties.get('duration_seconds')
        audio_file_link = file_properties.get('audio_file_link')
        
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
        title = st.text_area('What should this file be named? Keep it short!')

        # Multiselect for selecting contact who recorded the message
        who_recorded = st.multiselect(
            'Who recorded this? Only select one name.',
            options=list(contact_options.keys())
        )
        
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

        # --- Notes ---
        # Text area for entering notes to be added to the engagement
        action_items = st.text_area('Enter your action items here. Be specific!')
        
        # --- Submit ---
        # Button to submit the engagement to HubSpot
        if st.button('Submit'):

            # Initialize variables inside the submit block
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
                        else:
                            st.error(f"Failed to create company: {company_name}")
                    else:
                        st.warning(f"Company '{company_name}' already exists in HubSpot.")
                        company_id = company_options[existing_companies[0]]
                        new_company_ids.append(company_id)
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
                            else:
                                st.error(f"Failed to create contact: {full_name}")
                        else:
                            st.warning(f"Contact '{full_name}' already exists in HubSpot.")
                            contact_id = contact_options[existing_contacts[0]]
                            new_contact_ids.append(contact_id)
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

            # Update Google File Metadata (store only IDs)
            new_properties = {
                'transcription_timestamp': transcription_timestamp,
                'duration_seconds': str(duration_seconds),
                'audio_file_link': audio_file_link,
                'who_recorded_ids': ','.join(recorder_contact_ids),
                'file_title': title,
                'action_items': action_items,
                'contacts_linked_ids': ','.join(contact_ids),
                'companies_linked_ids': ','.join(company_ids),
                'new_contact_ids': ','.join(new_contact_ids),
                'new_company_ids': ','.join(new_company_ids),
            }

            gd_update_file_properties(file_id, new_properties)
            test_metadata = gd_get_file_properties(file_id)
            st.success(f"File metadata updated.")
            st.write(f"Metadata: {test_metadata}")
            
            # Rename file and move to processed gd folder 
            if who_recorded:
                recorder_name = who_recorded[0].split(' [')[0].upper()
                new_file_name = f"SIGNAL_{transcription_timestamp}_{recorder_name}_{title.upper()}_TRANSCRIPT__TAGGED.docx"
                gd_rename_file(file_id, new_file_name)

            gd_move_file_between_folders(file_id, TAGGED_TEXT_GD_FOLDER_ID)
            st.success(f"File moved to processed folder.")
            st.write(f"Folder ID: {TAGGED_TEXT_GD_FOLDER_ID}")

            # Write the data to HubSpot
            note_body = f"This entity was tagged in a transcription. The Google Drive link to the notes can be found here: {drive_link} \n\n Action Items: {action_items}"
            
            # Show a spinner while creating the engagement in HubSpot
            with st.spinner('Creating engagement...'):
                engagement_response = create_engagement(note_body, company_ids, contact_ids)
            
            # Check if the engagement creation was successful
            if engagement_response and 'engagement' in engagement_response:
                st.success("Link and notes have been successfully uploaded to HubSpot.")
            else:
                st.error("An error occurred while creating the engagement.")
            
    else:
        # Display an error message if the Google Drive link is invalid
        st.error("Invalid Google Drive or Google Docs link.")


        
