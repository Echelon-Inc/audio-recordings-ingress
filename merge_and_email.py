import json
import logging
import re
import io
import os
from datetime import datetime

import streamlit as st
import pandas as pd
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
import markdown

# ------------------------------
# Configuration and Initialization
# ------------------------------

# Set Streamlit page configuration
st.set_page_config(
    page_title="NOS | Generate Report",
    page_icon="Echelon_Icon_Sky Blue.png",
    layout="wide"
)

# Display Echelon logo
st.image("Echelon_Icon_Sky Blue.png", caption="The Home for Aliens", width=125)
st.title("NOS - Generate Report")
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

# TESTING
# GD_FOLDER_ID_TRANSCRIBED_TEXT = st.secrets["gdrive"]["GD_FOLDER_ID_TRANSCRIBED_TEXT_TEST"]
# GD_FOLDER_ID_TAGGED_TEXT = st.secrets["gdrive"]["GD_FOLDER_ID_TAGGED_TEXT_TEST"]
# GD_SPREADSHEET_ID_INGRESS_LOG = st.secrets["gdrive"]["GD_SPREADSHEET_ID_INGRESS_LOG_TEST"]

GD_SHEET_NAME_INGRESS_LOG_TRANSCRIBE = 'transcribe_audio'
GD_SHEET_NAME_INGRESS_LOG_TAG = 'tag_transcripts'
GD_SHEET_NAME_INGRESS_LOG_MERGED = 'merged_data'
UNIQUE_ID_COLUMN = 'gd_transcript_file_id'
MERGE_STATUS_TAG = 'merge_status_tag'
MERGE_STATUS_TRANSCRIBE = 'merge_status_transcribe'
SENT_FLAG_COLUMN = 'sent_flag'

# ------------------------------
# Define Helper Functions
# ------------------------------

def parse_entities(entities_str):
    """
    Parses a string of entities in the format 'Name [ID], Name [ID]' into a list of dictionaries.

    Args:
        entities_str (str): String representation of entities.

    Returns:
        list of dict: List of entities with 'name' and 'id' keys.
    """
    entities_list = []
    if entities_str:
        # Regex pattern to match 'Name [ID]'
        pattern = r'([^,\[\]]+?)\s*\[(\d+)\]'
        matches = re.findall(pattern, entities_str)
        for match in matches:
            name = match[0].strip()
            entity_id = match[1].strip()
            entities_list.append({'name': name, 'id': entity_id})
    return entities_list

def format_entities_with_links(entities_list, entity_type, hubspot_portal_id):
    """
    Formats entities with embedded hyperlinks to their HubSpot profiles.

    Args:
        entities_list (list of dict): List of entities with 'name' and 'id'.
        entity_type (str): 'contact' or 'company'.
        hubspot_portal_id (str): HubSpot portal ID.

    Returns:
        str: Formatted string with entities and hyperlinks.
    """
    formatted_entities = []
    for entity in entities_list:
        name = entity.get('name', '')
        entity_id = entity.get('id', '')
        if entity_id:
            if entity_type == 'contact':
                url = f"https://app.hubspot.com/contacts/{hubspot_portal_id}/contact/{entity_id}"
            elif entity_type == 'company':
                url = f"https://app.hubspot.com/contacts/{hubspot_portal_id}/company/{entity_id}"
            else:
                url = '#'
            formatted_entities.append(f"[{name}]({url})")
        else:
            formatted_entities.append(name)
    return ', '.join(formatted_entities)

def download_sheet_as_df(spreadsheet_id, sheet_name):
    """
    Downloads a Google Sheet and returns it as a pandas DataFrame.

    Args:
        spreadsheet_id (str): The ID of the Google Sheets document.
        sheet_name (str): The name of the sheet to download.

    Returns:
        pd.DataFrame: The sheet data as a DataFrame.
    """
    range_name = f"{sheet_name}!A:Z"  # Adjust columns as needed
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=range_name
    ).execute()
    values = result.get('values', [])
    if not values:
        logger.info(f"No data found in sheet {sheet_name}.")
        return pd.DataFrame()  # Return empty DataFrame if no data

    # Convert the data to DataFrame
    headers = values[0]
    rows = values[1:]

    num_columns = len(headers)

    # Ensure all rows have the same number of columns as headers
    for i, row in enumerate(rows):
        row_length = len(row)
        if row_length != num_columns:
            logger.warning(f"Row {i + 2} in sheet '{sheet_name}' has {row_length} columns; expected {num_columns}. Adjusting row.")
            if row_length < num_columns:
                # Pad the row with empty strings
                rows[i] = row + [''] * (num_columns - row_length)
            elif row_length > num_columns:
                # Truncate the row to match headers
                rows[i] = row[:num_columns]

    df = pd.DataFrame(rows, columns=headers)
    return df

def get_sheet_id(spreadsheet_id, sheet_name):
    """
    Retrieves the sheet ID of a Google Sheet by its name.

    Args:
        spreadsheet_id (str): The ID of the Google Sheets document.
        sheet_name (str): The name of the sheet.

    Returns:
        int: The sheet ID.
    """
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = spreadsheet.get("sheets", [])
    for sheet in sheets:
        if sheet.get("properties", {}).get("title") == sheet_name:
            return sheet.get("properties", {}).get("sheetId")
    raise ValueError(f"Sheet '{sheet_name}' not found.")

def get_column_index(spreadsheet_id, sheet_name, column_name):
    """
    Retrieves the index of a column in a Google Sheet based on the column name.

    Args:
        spreadsheet_id (str): The ID of the Google Sheets document.
        sheet_name (str): The name of the sheet.
        column_name (str): The name of the column.

    Returns:
        int: The column index (starting from 0), or None if not found.
    """
    range_name = f"{sheet_name}!A1:1"
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=range_name).execute()
    headers = result.get('values', [[]])[0]
    if column_name in headers:
        return headers.index(column_name)
    return None

def update_sheet(spreadsheet_id, sheet_name, df):
    """
    Updates a Google Sheet with the data from a DataFrame, overwriting existing data.

    Args:
        spreadsheet_id (str): The ID of the Google Sheets document.
        sheet_name (str): The name of the sheet to update.
        df (pd.DataFrame): The DataFrame containing the data.
    """
    df = df.fillna('')

    values = [df.columns.tolist()] + df.values.tolist()
    body = {
        'values': values
    }
    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A1",
        valueInputOption="RAW",
        body=body
    ).execute()
    st.write(f"Sheet '{sheet_name}' updated successfully.")

def update_merge_statuses(spreadsheet_id, sheet_name, unique_id_column, unique_ids, flag_column):
    """
    Updates the merge status in a Google Sheet for specified rows.

    Args:
        spreadsheet_id (str): The ID of the Google Sheets document.
        sheet_name (str): The name of the sheet to update.
        unique_id_column (str): The column containing the unique ID.
        unique_ids (list): List of unique IDs to mark as merged.
        flag_column (str): The column used to flag merged rows.
    """
    sheet_id = get_sheet_id(spreadsheet_id, sheet_name)
    range_name = f"{sheet_name}!A:Z"
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])

    if not values:
        st.write(f"No data found in sheet {sheet_name} to update.")
        return

    headers = values[0]
    rows = values[1:]
    unique_id_index = headers.index(unique_id_column)
    flag_index = headers.index(flag_column)

    requests = []
    for row_index, row in enumerate(rows, start=1):  # start=1 accounts for header row
        if len(row) <= unique_id_index:
            continue  # Skip rows without the unique_id
        if row[unique_id_index] in unique_ids:
            # Ensure the row has enough columns
            while len(row) <= flag_index:
                row.append('')
            # Only update if the flag is not already '1'
            if row[flag_index] != '1':
                requests.append({
                    "updateCells": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": row_index,
                            "endRowIndex": row_index + 1,
                            "startColumnIndex": flag_index,
                            "endColumnIndex": flag_index + 1,
                        },
                        "rows": [{
                            "values": [{
                                "userEnteredValue": {"stringValue": "1"}
                            }]
                        }],
                        "fields": "userEnteredValue"
                    }
                })

    if requests:
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()
        st.write(f"Merge statuses updated in '{sheet_name}' for {len(requests)} rows.")
    else:
        st.write(f"No merge status updates needed in '{sheet_name}'.")

def generate_markdown(report_data_list, hubspot_portal_id):
    """
    Generates markdown content from a list of report data dictionaries,
    embedding hyperlinks to HubSpot profiles for contacts and companies.

    Args:
        report_data_list (list of dict): List of report data dictionaries.
        hubspot_portal_id (str): HubSpot portal ID.

    Returns:
        str: Markdown content.
    """
    markdown_content = ""
    transcript_count = 1

    for row in report_data_list:
        # Extract necessary fields
        file_id = row.get('gd_transcript_file_id', '')
        transcript_title = row.get('transcript_title', 'Untitled Transcript')
        action_items = row.get('action_items', '').replace('\n', '  \n')

        # Extract who_recorded and datetime_uploaded
        who_recorded_str = row.get('who_recorded', '')
        datetime_uploaded = row.get('datetime_uploaded', '')

        # Parse who_recorded into name and ID
        who_recorded_list = parse_entities(who_recorded_str)
        # Since who_recorded is a single person, get the first entry
        if who_recorded_list:
            who_recorded_link = format_entities_with_links([who_recorded_list[0]], 'contact', hubspot_portal_id)
        else:
            who_recorded_link = who_recorded_str  # If parsing fails, display the original string

        # Extract contacts and companies data
        contacts_linked = row.get('contacts_linked', '')
        companies_linked = row.get('companies_linked', '')
        contacts_created = row.get('contacts_created', '')
        companies_created = row.get('companies_created', '')

        # Parse the contacts and companies into lists of dicts
        contacts_linked_list = parse_entities(contacts_linked)
        companies_linked_list = parse_entities(companies_linked)
        contacts_created_list = parse_entities(contacts_created)
        companies_created_list = parse_entities(companies_created)

        # Construct Google Drive link
        if file_id:
            drive_link = f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
        else:
            drive_link = "#"

        # Compile Markdown for the current transcript
        markdown_content += f"""
### Transcript {transcript_count}: [{transcript_title}]({drive_link})

**Who Recorded:** {who_recorded_link}  
**Datetime Uploaded:** {datetime_uploaded}  

**Existing Contacts Linked:** {format_entities_with_links(contacts_linked_list, 'contact', hubspot_portal_id)}  
**Existing Companies Linked:** {format_entities_with_links(companies_linked_list, 'company', hubspot_portal_id)}  
**New Contacts Linked:** {format_entities_with_links(contacts_created_list, 'contact', hubspot_portal_id)}  
**New Companies Linked:** {format_entities_with_links(companies_created_list, 'company', hubspot_portal_id)}  

**Action Items:**  
{action_items}

---
"""
        transcript_count += 1

    return markdown_content.strip()

def get_gmail_service():
    """
    Creates and returns a Gmail API service using OAuth2 credentials.
    """
    # Retrieve credentials from Streamlit secrets
    client_id = st.secrets["gmail"]["client_id"]
    client_secret = st.secrets["gmail"]["client_secret"]
    refresh_token = st.secrets["gmail"]["refresh_token"]

    # Define the required scopes
    SCOPES = ['https://www.googleapis.com/auth/gmail.send']

    # Create the Credentials object
    creds = Credentials(
        None,
        refresh_token=refresh_token,
        token_uri='https://oauth2.googleapis.com/token',
        client_id=client_id,
        client_secret=client_secret,
        scopes=SCOPES
    )

    try:
        # Refresh the access token if needed
        creds.refresh(Request())

        # Build the Gmail service
        service = build('gmail', 'v1', credentials=creds)
        logger.info("Gmail service created successfully.")
        return service
    except Exception as e:
        logger.exception(f"Failed to create Gmail service: {e}")
        return None

def create_message(sender, to, subject, markdown_content):
    """
    Creates a MIME message with both plain text and HTML parts from Markdown content.

    Args:
        sender (str): Sender's email address.
        to (str): Recipient's email address.
        subject (str): Subject of the email.
        markdown_content (str): The Markdown content to include in the email body.

    Returns:
        dict: The encoded email message ready to be sent via Gmail API.
    """
    try:
        # Convert Markdown to HTML
        html_content = markdown.markdown(markdown_content)

        # Create a multipart message and set headers
        message = MIMEMultipart("alternative")
        message["Subject"] = subject
        message["From"] = sender
        message["To"] = to

        # Attach the plain text and HTML versions of the email
        part1 = MIMEText(markdown_content, "plain")
        part2 = MIMEText(html_content, "html")

        message.attach(part1)
        message.attach(part2)

        # Encode the message in base64 URL-safe encoding
        raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

        return {"raw": raw_message}

    except Exception as e:
        logger.exception(f"Failed to create email message: {e}")
        return None

def send_email_via_gmail_api(service, message):
    """
    Sends an email using the Gmail API.

    Args:
        service (googleapiclient.discovery.Resource): The Gmail API service instance.
        message (dict): The encoded email message to send.

    Returns:
        bool: True if the email was sent successfully, False otherwise.
    """
    try:
        # Send the email
        send_message = service.users().messages().send(userId="me", body=message).execute()
        logger.info(f"Message Id: {send_message['id']} sent successfully.")
        return True
    except Exception as e:
        logger.exception(f"Failed to send email: {e}")
        return False

def send_email(markdown_content, sender_email, receiver_email):
    """
    Sends an email with the given markdown content.

    Args:
        markdown_content (str): The content of the email in markdown format.
        sender_email (str): Sender's email address.
        receiver_email (str): Receiver's email address.

    Returns:
        bool: True if email was sent successfully, False otherwise.
    """
    # Initialize the Gmail service
    gmail_service = get_gmail_service()

    if not gmail_service:
        logger.error("Gmail service could not be created. Email not sent.")
        return False

    subject = f"NOS Transcripts Report - {datetime.now().strftime('%Y-%m-%d')}"

    # Create the email message
    email_message = create_message(
        sender=sender_email,
        to=receiver_email,
        subject=subject,
        markdown_content=markdown_content
    )

    if not email_message:
        logger.error("Failed to create the email message. Email not sent.")
        return False

    # Send the email
    email_sent = send_email_via_gmail_api(gmail_service, email_message)

    if email_sent:
        logger.info("Email sent successfully!")
        return True
    else:
        logger.error("Failed to send the email.")
        return False

def send_emails(spreadsheet_id, merged_sheet_name):
    """
    Sends a single email containing all new entries in the merged_data sheet where sent_flag is '0'.

    Args:
        spreadsheet_id (str): The ID of the Google Sheets document.
        merged_sheet_name (str): The name of the merged_data sheet.
    """
    # Sender and receiver email addresses
    sender_email = st.secrets['email']['sender']
    receiver_email = st.secrets['email']['receiver']

    # Step 1: Download merged_data sheet
    merged_data_df = download_sheet_as_df(spreadsheet_id, merged_sheet_name)

    if merged_data_df.empty:
        st.write(f"No data found in sheet {merged_sheet_name}.")
        return

    # **Ensure 'sent_flag' column exists (should now always exist due to merging process)**
    if SENT_FLAG_COLUMN not in merged_data_df.columns:
        merged_data_df[SENT_FLAG_COLUMN] = ''

    # Step 2: Filter rows where sent_flag is '0'
    unsent_df = merged_data_df[merged_data_df[SENT_FLAG_COLUMN] == '0'].copy()

    if unsent_df.empty:
        st.write("No new emails to send.")
        return

    # Step 3: Generate email content for all unsent entries
    report_data_list = unsent_df.to_dict('records')
    markdown_content = generate_markdown(report_data_list, hubspot_portal_id)

    # Send email
    email_sent = send_email(markdown_content, sender_email, receiver_email)

    if email_sent:
        # Update sent_flag to '1' in the DataFrame for all sent entries
        merged_data_df.loc[unsent_df.index, SENT_FLAG_COLUMN] = '1'
        st.success(f"Email sent for {len(unsent_df)} entries.")
    else:
        st.write(f"Failed to send email.")

    # Step 4: Update merged_data sheet with updated sent_flag
    update_sheet(spreadsheet_id, merged_sheet_name, merged_data_df)

def merge_data():
    # Step 1: Download data from both sheets
    df_tag = download_sheet_as_df(GD_SPREADSHEET_ID_INGRESS_LOG, GD_SHEET_NAME_INGRESS_LOG_TAG)
    df_transcribe = download_sheet_as_df(GD_SPREADSHEET_ID_INGRESS_LOG, GD_SHEET_NAME_INGRESS_LOG_TRANSCRIBE)

    if df_tag.empty or df_transcribe.empty:
        st.write("No data to process.")
        return

    # Ensure merge_status columns exist
    if MERGE_STATUS_TAG not in df_tag.columns:
        df_tag[MERGE_STATUS_TAG] = '0'
    if MERGE_STATUS_TRANSCRIBE not in df_transcribe.columns:
        df_transcribe[MERGE_STATUS_TRANSCRIBE] = '0'

    # Step 2: Merge data where both merge statuses are '0'
    merged_df = pd.merge(df_tag, df_transcribe, on=UNIQUE_ID_COLUMN, how='inner')
    merged_df = merged_df[(merged_df[MERGE_STATUS_TAG] == '0') & (merged_df[MERGE_STATUS_TRANSCRIBE] == '0')].copy()

    if merged_df.empty:
        st.write("No new data to merge.")
    else:
        # Set merge_status columns to '1' in merged_df
        merged_df[MERGE_STATUS_TAG] = '1'
        merged_df[MERGE_STATUS_TRANSCRIBE] = '1'

        # **Add sent_flag column and set to '0' for new rows**
        merged_df[SENT_FLAG_COLUMN] = '0'

        # Step 3: Download existing merged_data sheet
        merged_data_existing = download_sheet_as_df(GD_SPREADSHEET_ID_INGRESS_LOG, GD_SHEET_NAME_INGRESS_LOG_MERGED)
        if merged_data_existing.empty:
            merged_data_combined = merged_df
        else:
            merged_data_combined = pd.concat([merged_data_existing, merged_df], ignore_index=True)

        # Step 4: Update merged_data sheet
        update_sheet(GD_SPREADSHEET_ID_INGRESS_LOG, GD_SHEET_NAME_INGRESS_LOG_MERGED, merged_data_combined)

        # Step 5: Update merge statuses in original sheets
        unique_ids = merged_df[UNIQUE_ID_COLUMN].tolist()
        update_merge_statuses(GD_SPREADSHEET_ID_INGRESS_LOG, GD_SHEET_NAME_INGRESS_LOG_TAG, UNIQUE_ID_COLUMN, unique_ids, MERGE_STATUS_TAG)
        update_merge_statuses(GD_SPREADSHEET_ID_INGRESS_LOG, GD_SHEET_NAME_INGRESS_LOG_TRANSCRIBE, UNIQUE_ID_COLUMN, unique_ids, MERGE_STATUS_TRANSCRIBE)

        st.success("Merging process completed.")

# ------------------------------
# Main Function
# ------------------------------

st.write("**Click the button below to generate an email report for all recently tagged transcripts**")
st.write("⚠️ONLY CLICK THIS BUTTON WHEN YOU ARE DONE WITH YOUR FULL TAGGING SESSION!⚠️")

if st.button('Generate Report'):
    merge_data()
    send_emails(GD_SPREADSHEET_ID_INGRESS_LOG, GD_SHEET_NAME_INGRESS_LOG_MERGED)
    st.write("Email sent. You can close this tab now.")
    st.stop()

# ------------------------------
# Additional Notes
# ------------------------------
st.markdown("---")  # Add a separator at the bottom
st.write("© 2024 Echelon NOS. All rights reserved.")