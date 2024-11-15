import requests
import datetime as dt
from datetime import timedelta
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from googleapiclient.discovery import build
import base64
import streamlit as st
import pytz
import urllib.parse
import difflib  # For smart suggestions

# ------------------------------
# Configuration and Initialization
# ------------------------------

# Set Streamlit page configuration
st.set_page_config(
    page_title="NOS | Happy Minute",
    page_icon="Echelon_Icon_Sky Blue.png",
    layout="wide"
)

# Display Echelon logo
st.image("Echelon_Icon_Sky Blue.png", caption="The Home for Aliens", width=125)
st.title("NOS - Happy Minute")
st.write("Custom Built for Kerri Faber")

# ------------------------------
# Define Google API Scopes and Initialize Clients
# ------------------------------
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets'
]
gcp_secrets = st.secrets["gcp_service_account"]
creds = service_account.Credentials.from_service_account_info(
    gcp_secrets,
    scopes=SCOPES
)
sheets_service = build('sheets', 'v4', credentials=creds)

# ------------------------------
# Define HubSpot Credentials and Headers
# ------------------------------
HUBSPOT_API_TOKEN = st.secrets["hubspot"]["api_token"]
headers = {
    "Authorization": f"Bearer {HUBSPOT_API_TOKEN}",
    "Content-Type": "application/json"
}

# ------------------------------
# Define Google Drive Folder and Spreadsheet IDs
# ------------------------------
# PRODUCTION
GD_SPREADSHEET_ID_INGRESS_LOG = st.secrets["gdrive"]["GD_SPREADSHEET_ID_INGRESS_LOG_PROD"]

# TESTING
# GD_SPREADSHEET_ID_INGRESS_LOG = st.secrets["gdrive"]["GD_SPREADSHEET_ID_INGRESS_LOG_TEST"]

GD_SHEET_NAME_INGRESS_LOG = 'happy_minute'
GD_SHEET_NAME_SUMMARY_LOG = 'happy_minute_summary'

# ------------------------------
# HubSpot API Functions
# ------------------------------

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

def create_note_in_hubspot(note_body, hs_timestamp):
    """
    Creates a Note in HubSpot with the given body content and timestamp.
    """
    url = "https://api.hubapi.com/crm/v3/objects/notes"
    data = {
        "properties": {
            "hs_note_body": note_body,
            "hs_timestamp": hs_timestamp
        }
    }
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        note = response.json()
        note_id = note.get('id')
        return note_id
    except Exception as e:
        st.error(f"Error creating note in HubSpot: {e}")
        return None

def associate_note_with_contact(note_id, contact_id):
    """
    Associates the created Note with specified contact.
    """
    url = f"https://api.hubapi.com/crm/v3/objects/notes/{note_id}/associations/contacts/{contact_id}/note_to_contact"
    try:
        response = requests.put(url, headers=headers)
        response.raise_for_status()
        return True
    except Exception as e:
        st.error(f"Error associating note with contact ID {contact_id}: {e}")
        return False

# ------------------------------
# Existing Functions (Adjusted as Needed)
# ------------------------------

def log_participants_to_google_sheet_with_new_columns(date, participants, sheets_service, spreadsheet_id, sheet_name):
    rows = []
    for participant in participants:
        # Prepare each row with desired data
        row = [
            date.strftime('%Y-%m-%d'),                # Column A: Date
            participant.get('name'),                  # Column B: Participant Name
            participant.get('join_time'),             # Column C: Join Time
            participant.get('contact_name'),          # Column E: HubSpot Contact Name
            participant.get('contact_id'),            # Column F: HubSpot Contact ID
            participant.get('new_contact_created')    # Column G: New Contact Created (Yes/No)
        ]
        rows.append(row)
    
    # Write rows to Google Sheets
    try:
        request = sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f'{sheet_name}!A:H',  # Specify columns A to H
            valueInputOption='RAW',
            insertDataOption='INSERT_ROWS',
            body={'values': rows}
        )
        request.execute()
        st.success("Logged participant data to the spreadsheet.")
    except Exception as e:
        st.error(f"Error writing to spreadsheet: {str(e)}")

def log_event_to_google_sheet(date, raw_attendees, existing_contacts_linked, new_contacts_created, sheets_service, spreadsheet_id, sheet_name, description, retrospective):
    # Prepare the row data
    row = [
        date.strftime('%Y-%m-%d'),                    # Column A: Event Date
        ', '.join(raw_attendees),                     # Column B: Raw Attendees
        ', '.join(existing_contacts_linked),          # Column C: Existing HubSpot Profiles Linked
        ', '.join(new_contacts_created),              # Column D: New HubSpot Profiles Created
        len(existing_contacts_linked),                # Column E: Count of Existing Contacts Linked
        len(new_contacts_created),                    # Column F: Count of New Contacts Created
        description,
        retrospective
    ]
    # Write the row to Google Sheets
    try:
        request = sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f'{sheet_name}!A:F',  # Specify columns A to F
            valueInputOption='RAW',
            insertDataOption='INSERT_ROWS',
            body={'values': [row]}
        )
        request.execute()
        st.success("Logged event data to the event log.")
    except Exception as e:
        st.error(f"Error writing to event log spreadsheet: {str(e)}")

def get_zoom_access_token():
    # Retrieve credentials from st.secrets
    account_id = st.secrets["zoom"]["account_id"]
    client_id = st.secrets["zoom"]["client_id"]
    client_secret = st.secrets["zoom"]["client_secret"]
    
    # Encode client_id and client_secret for Basic Authentication
    credentials = f"{client_id}:{client_secret}"
    credentials_bytes = credentials.encode('ascii')
    base64_credentials = base64.b64encode(credentials_bytes).decode('ascii')
    
    # Prepare the request to obtain the access token
    url = f"https://zoom.us/oauth/token?grant_type=account_credentials&account_id={account_id}"
    headers = {
        "Authorization": f"Basic {base64_credentials}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    # Make the POST request to get the access token
    response = requests.post(url, headers=headers)
    
    # Check if the request was successful
    if response.status_code == 200:
        access_token = response.json().get("access_token")
        return access_token
    else:
        st.error(f"Failed to obtain access token: {response.status_code} - {response.text}")
        return None

def get_past_meeting_instances(access_token, meeting_id):
    url = f"https://api.zoom.us/v2/past_meetings/{meeting_id}/instances"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        instances = response.json().get("meetings", [])
        return instances
    else:
        st.error(f"Failed to get past meeting instances: {response.status_code} - {response.text}")
        return []

def get_meeting_participants(access_token, meeting_uuid):
    url = f"https://api.zoom.us/v2/past_meetings/{meeting_uuid}/participants"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    params = {
        "page_size": 300  # Adjust as needed
    }
    participants = []
    next_page_token = ''
    
    while True:
        if next_page_token:
            params['next_page_token'] = next_page_token
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            participants.extend(data.get('participants', []))
            next_page_token = data.get('next_page_token', '')
            if not next_page_token:
                break
        else:
            st.error(f"Failed to get participants: {response.status_code} - {response.text}")
            break
    return participants

def run_script():
    access_token = get_zoom_access_token()
    meeting_id = st.secrets["zoom"]["meeting_id"]
    if not access_token:
        return None, None
    
    today_utc = dt.datetime.now(pytz.utc)
    days_since_friday = (today_utc.weekday() - 4) % 7
    if days_since_friday == 0:
        desired_date_friday = today_utc
    else:
        desired_date_friday = today_utc - timedelta(days=days_since_friday)
    
    desired_date_saturday = desired_date_friday + timedelta(days=1)
    time_friday = dt.time(23, 0)
    time_saturday = dt.time(2, 0)

    # Adjust the datetime combination to ensure timezone awareness
    start_time_utc = pytz.utc.localize(dt.datetime.combine(desired_date_friday.date(), time_friday))
    end_time_utc = pytz.utc.localize(dt.datetime.combine(desired_date_saturday.date(), time_saturday))
    
    all_instances = get_past_meeting_instances(access_token, meeting_id)
    if not all_instances:
        st.error("No past meeting instances found.")
        return None, None
    
    target_instances = []
    for instance in all_instances:
        instance_start_time_str = instance.get('start_time')
        instance_start_time = dt.datetime.strptime(instance_start_time_str, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.utc)
        if start_time_utc <= instance_start_time <= end_time_utc:
            target_instances.append(instance)
    
    if not target_instances:
        st.error("No meeting instances found within the specified time window.")
        return None, None
    
    # Gather all participants for target instances
    participants_data = []
    for target_instance in target_instances:
        meeting_uuid = target_instance.get('uuid')
        meeting_uuid_encoded = urllib.parse.quote(meeting_uuid, safe='')
        participants = get_meeting_participants(access_token, meeting_uuid_encoded)
    
        for participant in participants:
            join_time_str = participant.get('join_time')
            join_time = dt.datetime.strptime(join_time_str, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.utc)
            if start_time_utc <= join_time <= end_time_utc:
                participant_data = {
                    'name': participant.get('name'),
                    'join_time': join_time_str
                }
                participants_data.append(participant_data)
    
    return participants_data, desired_date_friday

# ------------------------------
# Main Streamlit Application
# ------------------------------

def main():
    
    # 1. Let the user provide a description of the event
    event_description = st.text_input("Describe our most recent Happy Minute! Who hosted? Who was toasted? Why?")
    
    # 2. Let the user write a paragraph on the perceived quality of the event
    event_retrospective = st.text_area("Provide commentary on the event. What worked? What didn't? Was it a good HM?")
    
    # Initialize session state variables if they don't exist
    if 'participants_data' not in st.session_state:
        st.session_state['participants_data'] = None
    if 'desired_date_friday' not in st.session_state:
        st.session_state['desired_date_friday'] = None
    if 'contacts_data' not in st.session_state:
        st.session_state['contacts_data'] = None

    if st.button("Click to log HM attendees"):
        participants_data, desired_date_friday = run_script()
        if participants_data:
            st.session_state['participants_data'] = participants_data
            st.session_state['desired_date_friday'] = desired_date_friday
        else:
            st.error("No participant data retrieved.")
    
    # Fetch contacts from HubSpot
    if st.session_state['participants_data'] and st.session_state['contacts_data'] is None:
        with st.spinner('Fetching contacts from HubSpot...'):
            st.session_state['contacts_data'] = get_all_contacts()
    
    if st.session_state['participants_data']:
        # Retrieve contacts data from session state
        contacts_data = st.session_state['contacts_data']

        # Create a dictionary for contacts with "firstname lastname [ID]" as the key and ID as the value
        contact_options = {
            f"{contact.get('properties', {}).get('firstname', '')} {contact.get('properties', {}).get('lastname', '')} [{contact.get('id')}]": contact.get('id')
            for contact in contacts_data
        }
        # Also create a list of contact names for matching
        contact_names_list = [f"{contact.get('properties', {}).get('firstname', '')} {contact.get('properties', {}).get('lastname', '')}" for contact in contacts_data]
        contact_name_to_id = {f"{contact.get('properties', {}).get('firstname', '')} {contact.get('properties', {}).get('lastname', '')}": contact.get('id') for contact in contacts_data}

        # Provide a disclaimer for duplicate names
        st.write("**Note:** If there are duplicate names in the selection lists, please refer to the contact ID in brackets to verify the correct contact in HubSpot.")

        st.write("Please enter/select the full names for the participants:")
        for idx, participant in enumerate(st.session_state['participants_data']):
            participant_name = participant.get('name')
            st.write(f"Original Name: {participant_name}")
            key_prefix = f"participant_{idx}"

            # Use an expander for each participant to keep the UI clean
            with st.expander(f"Participant {idx+1}: {participant_name}"):
                # Smart suggestion of existing contact
                # Use difflib to find close matches
                close_matches = difflib.get_close_matches(participant_name, contact_names_list, n=3, cutoff=0.6)
                suggested_contact_options = [f"{name} [{contact_name_to_id[name]}]" for name in close_matches if name in contact_name_to_id]
                # Option to select existing contact or create new
                contact_selection = st.radio(
                    f"Select an option for '{participant_name}':",
                    options=["Select an existing contact", "Create new contact"],
                    key=f"{key_prefix}_contact_option"
                )

                if contact_selection == "Select an existing contact":
                    # Select from existing contacts with suggestions
                    selected_contact = st.selectbox(
                        "Choose a contact:",
                        options=[""] + suggested_contact_options + list(set(contact_options.keys()) - set(suggested_contact_options)),
                        key=f"{key_prefix}_existing_contact"
                    )
                    if selected_contact:
                        participant['contact_id'] = contact_options[selected_contact]
                        participant['contact_name'] = selected_contact
                        participant['new_contact_created'] = "No"
                    else:
                        participant['contact_id'] = None
                        participant['contact_name'] = None
                else:
                    # Create new contact
                    new_contact_fullname = st.text_input(
                        "Enter full name for new contact:",
                        key=f"{key_prefix}_new_contact_name"
                    )
                    participant['new_contact_fullname'] = new_contact_fullname.strip()
                    participant['contact_id'] = None  # Will be set upon creation
                    participant['contact_name'] = new_contact_fullname.strip()
                    participant['new_contact_created'] = "Yes"

                # Handle descriptions and join times as before
                if '[1]' in participant_name or '[2]' in participant_name:
                    participant['join_time'] = 'N/A'
                    participant['description'] = event_description
                else:
                    participant['description'] = ''

        # --- Add Additional Contacts Section ---
        st.header("Add Additional Contacts")
        st.write("If there are additional contacts who attended but are not listed above, you can add them here.")

        # Multiselect for existing contacts
        additional_existing_contacts = st.multiselect(
            'Select existing contacts to add:',
            options=list(contact_options.keys()),
            key='additional_existing_contacts'
        )

        # Text area for new contacts
        st.write("Enter names of new contacts to create in HubSpot (one per line):")
        additional_new_contacts_input = st.text_area(
            'New contacts:',
            key='additional_new_contacts_input'
        )

        # Prepare additional contacts data
        st.session_state['additional_contacts_data'] = []
        # Process existing contacts
        for contact_name in additional_existing_contacts:
            contact_id = contact_options[contact_name]
            st.session_state['additional_contacts_data'].append({
                'name': None,  # No original participant name
                'join_time': None,
                'description': '',
                'contact_name': contact_name,
                'contact_id': contact_id,
                'new_contact_created': "No"
            })
        # Process new contacts
        additional_new_contacts = [name.strip() for name in additional_new_contacts_input.strip().split('\n') if name.strip()]
        for fullname in additional_new_contacts:
            st.session_state['additional_contacts_data'].append({
                'name': None,
                'join_time': None,
                'description': '',
                'contact_name': fullname,
                'new_contact_fullname': fullname,
                'contact_id': None,  # Will be set upon creation
                'new_contact_created': "Yes"
            })

        # Add a "Log to Spreadsheet" button
        if st.button("Log to Spreadsheet and Link to HubSpot!"):
            # Retrieve the updated participants data from session state
            participants_data = st.session_state['participants_data']
            additional_contacts_data = st.session_state.get('additional_contacts_data', [])
            desired_date_friday = st.session_state['desired_date_friday']

            # Combine participants and additional contacts
            all_contacts_data = participants_data + additional_contacts_data

            # Create new contacts in HubSpot if needed
            for participant in all_contacts_data:
                if participant.get('new_contact_created') == "Yes":
                    fullname = participant.get('new_contact_fullname')
                    if fullname:
                        names = fullname.strip().split()
                        if len(names) >= 2:
                            firstname = ' '.join(names[:-1])
                            lastname = names[-1]
                            contact_response = create_contact(firstname, lastname)
                            if contact_response and 'id' in contact_response:
                                contact_id = contact_response['id']
                                participant['contact_id'] = contact_id
                                participant['contact_name'] = f"{firstname} {lastname} [{contact_id}]"
                            else:
                                st.error(f"Failed to create contact: {fullname}")
                        else:
                            st.error(f"Invalid contact name format: '{fullname}'. Each contact must include at least a first name and a last name.")
                    else:
                        st.error("Full name for new contact is required.")
                else:
                    participant['new_contact_created'] = "No"  # Ensure consistency

            # Now, create a note in HubSpot
            friday_date = desired_date_friday.date()
            note_body = f"<b>HAPPY MINUTE:</b> {friday_date}<br><b>Description:</b> {event_description}<br><b>Retrospective:</b> {event_retrospective}"
            hs_timestamp = int(dt.datetime.now().timestamp() * 1000)  # Current time in milliseconds
            note_id = create_note_in_hubspot(note_body, hs_timestamp)

            if note_id:
                # Associate note with each participant's contact
                for participant in all_contacts_data:
                    contact_id = participant.get('contact_id')
                    if contact_id:
                        success = associate_note_with_contact(note_id, contact_id)
                        if not success:
                            st.error(f"Failed to associate note with contact ID {contact_id}")
            else:
                st.error("Failed to create note in HubSpot.")

            # Update Google Sheets
            log_participants_to_google_sheet_with_new_columns(
                friday_date,
                all_contacts_data,
                sheets_service,
                GD_SPREADSHEET_ID_INGRESS_LOG,
                GD_SHEET_NAME_INGRESS_LOG
            )

            # Prepare data for the secondary event log
            raw_attendees = [p.get('name') for p in participants_data if p.get('name')]
            existing_contacts_linked = [p.get('contact_name') for p in all_contacts_data if p.get('new_contact_created') == "No" and p.get('contact_name')]
            new_contacts_created = [p.get('contact_name') for p in all_contacts_data if p.get('new_contact_created') == "Yes" and p.get('contact_name')]

            # Log to the secondary event log
            log_event_to_google_sheet(
                friday_date,
                raw_attendees,
                existing_contacts_linked,
                new_contacts_created,
                sheets_service,
                GD_SPREADSHEET_ID_INGRESS_LOG,
                GD_SHEET_NAME_SUMMARY_LOG, 
                event_description, 
                event_retrospective
            )

            st.success("All data logged successfully.")

            # Clear the session state after logging
            st.session_state['participants_data'] = None
            st.session_state['desired_date_friday'] = None
            st.session_state['additional_contacts_data'] = None
            # Optionally, reset clean names
            for idx in range(len(participants_data)):
                key_prefix = f"participant_{idx}"
                keys_to_delete = [f"{key_prefix}_contact_option", f"{key_prefix}_existing_contact", f"{key_prefix}_new_contact_name"]
                for key in keys_to_delete:
                    if key in st.session_state:
                        del st.session_state[key]
            # Clear additional contacts inputs
            if 'additional_existing_contacts' in st.session_state:
                del st.session_state['additional_existing_contacts']
            if 'additional_new_contacts_input' in st.session_state:
                del st.session_state['additional_new_contacts_input']

if __name__ == "__main__":
    main()

# ------------------------------
# Additional Notes
# ------------------------------
st.markdown("---")  # Add a separator at the bottom
st.write("© 2024 Echelon NOS. All rights reserved.")