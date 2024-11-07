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
import markdown

# ------------------------------
# Configuration and Initialization
# ------------------------------

# Set Streamlit page configuration
st.set_page_config(
    page_title="NOS Transcript Tagging",
    page_icon="Echelon_Icon_Sky Blue.png",
    layout="wide"
)

# Display Echelon logo
st.image("Echelon_Icon_Sky Blue.png", caption="The Home for Aliens", width=125)
st.title("NOS - Tag Transcripts")
st.write("Custom Built for Kerri Faber")

# ------------------------------
# Initialize Session State
# ------------------------------
if 'transcriptions_log' not in st.session_state:
    st.session_state['transcriptions_log'] = []

# UI State Flags
if 'show_form' not in st.session_state:
    st.session_state['show_form'] = True  # Show form initially

if 'show_buttons' not in st.session_state:
    st.session_state['show_buttons'] = False  # Hide additional buttons initially

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
# GD_FOLDER_ID_TRANSCRIBED_TEXT = st.secrets["gdrive"]["GD_FOLDER_ID_TRANSCRIBED_TEXT_PROD"]
# GD_FOLDER_ID_TAGGED_TEXT = st.secrets["gdrive"]["GD_FOLDER_ID_TAGGED_TEXT_PROD"]
# GD_SPREADSHEET_ID_INGRESS_LOG = st.secrets["gdrive"]["GD_SPREADSHEET_ID_INGRESS_LOG_PROD"]
# GD_SHEET_NAME_INGRESS_LOG = 'tag_transcripts'

# TESTING
GD_FOLDER_ID_TRANSCRIBED_TEXT = st.secrets["gdrive"]["GD_FOLDER_ID_TRANSCRIBED_TEXT_TEST"]
GD_FOLDER_ID_TAGGED_TEXT = st.secrets["gdrive"]["GD_FOLDER_ID_TAGGED_TEXT_TEST"]
GD_SPREADSHEET_ID_INGRESS_LOG = st.secrets["gdrive"]["GD_SPREADSHEET_ID_INGRESS_LOG_TEST"]
GD_SHEET_NAME_INGRESS_LOG = 'tag_transcripts'

def pull_report_data():
    """
    Fetches data from a specified Google Sheet, filters rows where the 'Sent Flag' is 'No',
    structures the data into dictionaries, and returns the data as a list of dictionaries.

    Returns:
        list of dict: A list where each dictionary represents a row with key-value pairs corresponding
                      to the column headers and their respective values.
    """
    # === Configuration ===
    SPREADSHEET_ID = GD_SPREADSHEET_ID_INGRESS_LOG  
    SHEET_NAME = GD_SHEET_NAME_INGRESS_LOG          
    RANGE_NAME = f"{SHEET_NAME}!A1:J"                 # Columns A to J

    # === Expected Column Headers ===
    EXPECTED_HEADERS = [
        'gd_transcript_file_id',                # Column A
        'datetime_tagged',                      # Column B
        'transcript_title',                     # Column C
        'who_recorded',               # Column D
        'action_items',                         # Column E
        'contacts_linked',            # Column F
        'companies_linked',           # Column G
        'contacts_created',           # Column H
        'companies_created',          # Column I
        'sent_flag'                             # Column J
    ]

    try:
        # === Retrieve Data from Google Sheets ===
        logger.info("Fetching data from Google Sheets...")
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME
        ).execute()

        values = result.get('values', [])

        if not values:
            logger.warning("No data found in the sheet.")
            return []

        # === Extract Headers and Data Rows ===
        headers = values[0]
        data_rows = values[1:]

        logger.info(f"Number of data rows fetched: {len(data_rows)}")

        # === Validate Headers ===
        missing_headers = [header for header in EXPECTED_HEADERS if header not in headers]
        if missing_headers:
            error_message = f"The following expected columns are missing in the sheet: {missing_headers}"
            logger.error(error_message)
            return []

        # Map header names to their indices for dynamic access
        header_indices = {header: index for index, header in enumerate(headers)}

        # === Process Rows ===
        report_data = []

        for row_number, row in enumerate(data_rows, start=2):  # Start at 2 considering header
            # Safeguard against incomplete rows
            if len(row) < len(EXPECTED_HEADERS):
                logger.warning(f"Row {row_number} is incomplete. Expected {len(EXPECTED_HEADERS)} columns, got {len(row)}. Skipping.")
                continue

            # Retrieve the 'sent_flag' value and check if it's 'No'
            sent_flag = row[header_indices['sent_flag']].strip().lower()
            if sent_flag == 'no':
                # Construct the data dictionary
                row_data = {
                    'gd_transcript_file_id': row[header_indices['gd_transcript_file_id']].strip(),
                    'datetime_tagged': row[header_indices['datetime_tagged']].strip(),
                    'transcript_title': row[header_indices['transcript_title']].strip(),
                    'who_recorded': row[header_indices['who_recorded']].strip(),
                    'action_items': row[header_indices['action_items']].strip(),
                    'contacts_linked': row[header_indices['contacts_linked']].strip(),
                    'companies_linked': row[header_indices['companies_linked']].strip(),
                    'contacts_created': row[header_indices['contacts_created']].strip(),
                    'companies_created': row[header_indices['companies_created']].strip(),
                    'sent_flag': row[header_indices['sent_flag']].strip()
                }

                report_data.append(row_data)
                logger.info(f"Row {row_number} added to report data.")

        logger.info(f"Total rows with 'sent_flag' as 'No': {len(report_data)}")
        return report_data

    except Exception as e:
        logger.exception(f"An error occurred while pulling report data: {e}")
        return []


def parse_entities(entity_str):
    """
    Parses a string containing entities in the format "Name [ID], Name [ID], ..."
    
    Args:
        entity_str (str): The string to parse.
    
    Returns:
        list of tuples: A list where each tuple contains (name, id).
    """
    import re
    entities = [e.strip() for e in entity_str.split(",")]
    result = []
    for e in entities:
        match = re.match(r"^(.*?)\s*\[(\d+)\]$", e)
        if match:
            name = match.group(1).strip()
            id = match.group(2).strip()
            result.append((name, id))
        else:
            # Handle unexpected formats gracefully
            result.append((e, None))
    return result

def format_hubspot_contact_link(name, contact_id, portal_id):
    """
    Formats a contact's name and ID into a Markdown link to their HubSpot profile.
    
    Args:
        name (str): The contact's name.
        contact_id (str): The contact's unique HubSpot ID.
        portal_id (str): Your HubSpot portal ID.
    
    Returns:
        str: A Markdown-formatted link.
    """
    if contact_id:
        url = f"https://app.hubspot.com/contacts/{portal_id}/contact/{contact_id}/"
        return f"[{name}]({url})"
    else:
        return name  # Return the name without a link if ID is missing

def format_hubspot_company_link(name, company_id, portal_id):
    """
    Formats a company's name and ID into a Markdown link to their HubSpot profile.
    
    Args:
        name (str): The company's name.
        company_id (str): The company's unique HubSpot ID.
        portal_id (str): Your HubSpot portal ID.
    
    Returns:
        str: A Markdown-formatted link.
    """
    if company_id:
        url = f"https://app.hubspot.com/contacts/{portal_id}/company/{company_id}/"
        return f"[{name}]({url})"
    else:
        return name  # Return the name without a link if ID is missing

def generate_markdown(report_data, hubspot_portal_id):
    """
    Converts a list of dictionaries into a structured Markdown string.
    
    Args:
        report_data (list of dict): The data extracted from Google Sheets.
        hubspot_portal_id (str): Your HubSpot portal ID for constructing profile links.
    
    Returns:
        str: A Markdown-formatted string.
    """
    markdown_content = ""
    transcript_count = 1
    
    for row in report_data:
        # Extract necessary fields
        file_id = row.get('gd_transcript_file_id', '')
        datetime_tagged = row.get('datetime_tagged', '')
        transcript_title = row.get('transcript_title', 'Untitled Transcript')
        who_recorded = row.get('who_recorded', '')
        action_items = row.get('action_items', '').replace('\n', '  \n')  # Updated line
        contacts_linked = row.get('contacts_linked', '')
        companies_linked = row.get('companies_linked', '')
        contacts_created = row.get('contacts_created', '')
        companies_created = row.get('companies_created', '')
        
        # Construct Google Drive link
        if file_id:
            drive_link = f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
            transcript_line = f"**Transcript {transcript_count}:** [{transcript_title}]({drive_link})"
        else:
            transcript_line = f"**Transcript {transcript_count}:** {transcript_title}"
        
        # Parse and format Contacts Linked
        contacts_linked_list = parse_entities(contacts_linked)
        contacts_linked_md = ", ".join([
            format_hubspot_contact_link(name, contact_id, hubspot_portal_id) 
            for name, contact_id in contacts_linked_list
        ]) if contacts_linked_list else "None"
        
        # Parse and format Companies Linked
        companies_linked_list = parse_entities(companies_linked)
        companies_linked_md = ", ".join([
            format_hubspot_company_link(name, company_id, hubspot_portal_id) 
            for name, company_id in companies_linked_list
        ]) if companies_linked_list else "None"
        
        # Parse and format Contacts Created
        contacts_created_list = parse_entities(contacts_created)
        contacts_created_md = ", ".join([
            format_hubspot_contact_link(name, contact_id, hubspot_portal_id) 
            for name, contact_id in contacts_created_list
        ]) if contacts_created_list else "None"
        
        # Parse and format Companies Created
        companies_created_list = parse_entities(companies_created)
        companies_created_md = ", ".join([
            format_hubspot_company_link(name, company_id, hubspot_portal_id) 
            for name, company_id in companies_created_list
        ]) if companies_created_list else "None"
        
        # Compile Markdown for the current transcript
        markdown_content += f"""
### Transcript {transcript_count}: [{transcript_title}]({drive_link})

**Contacts Linked:** {contacts_linked_md}  
**Companies Linked:** {companies_linked_md}  
**Contacts Created:** {contacts_created_md}  
**Companies Created:** {companies_created_md}  

**Action Items:**  
{action_items}

---
"""
        transcript_count += 1
    
    return markdown_content.strip()


def get_gmail_service():
    """
    Creates and returns a Gmail API service using OAuth2 credentials.
    
    Returns:
        service (googleapiclient.discovery.Resource): The Gmail API service instance.
    """
    # Retrieve credentials from Streamlit secrets
    client_id = st.secrets["gmail"]["client_id"]
    client_secret = st.secrets["gmail"]["client_secret"]
    refresh_token = st.secrets["gmail"]["refresh_token"]

    # client_id = '112778253121-sbgcvenjbeg7bf1grf977chirgnormad.apps.googleusercontent.com'
    # client_secret = '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'
    # refresh_token = '1//04Thhksa74T-PCgYIARAAGAQSNwF-L9Irjx7gMlsMTgixsiGkeXnhX8tT7yEQsMQ8ysC48NyHmT1LKuPBJ1tL77lmuwsgbwqyPgk'
    
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


# === Example Usage ===
if __name__ == "__main__":
    # Fetch the report data
    report = pull_report_data()
    print(json.dumps(report, indent=4))  
    if not report:
        logger.warning("No data available to generate the Markdown report.")
        print("No data available to generate the Markdown report.")
    else:
                
        # Generate the Markdown content
        markdown_content = generate_markdown(report, hubspot_portal_id)
        
        # Define the output file path (optional)
        output_file = "transcripts_report.md"
        
        try:
            # Write the Markdown content to the file
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(markdown_content)
            
            logger.info(f"Markdown report successfully saved to '{output_file}'.")
            print(f"Markdown report successfully saved to '{output_file}'.")
        
        except Exception as e:
            logger.exception(f"Failed to write Markdown report to '{output_file}': {e}")
            print(f"Failed to write Markdown report to '{output_file}': {e}")
        
        # === Sending the Email via Gmail API ===
        
        # Initialize the Gmail service
        gmail_service = get_gmail_service()
        
        if not gmail_service:
            logger.error("Gmail service could not be created. Email not sent.")
            print("Gmail service could not be created. Email not sent.")
        else:
            # Define email parameters
            subject = f"NOS Transcripts Report - {datetime.now().strftime('%Y-%m-%d')}"
            sender_email = st.secrets["gmail"]["email_sender"]       # e.g., "your_email@example.com"
            receiver_email = st.secrets["gmail"]["email_receiver"]   # e.g., "recipient@example.com"
            
            # Create the email message
            email_message = create_message(
                sender=sender_email,
                to=receiver_email,
                subject=subject,
                markdown_content=markdown_content
            )
            
            if not email_message:
                logger.error("Failed to create the email message. Email not sent.")
                print("Failed to create the email message. Email not sent.")
            else:
                # Send the email
                email_sent = send_email_via_gmail_api(gmail_service, email_message)
                
                if email_sent:
                    logger.info("Email sent successfully!")
                    print("Email sent successfully!")
                else:
                    logger.error("Failed to send the email.")
                    print("Failed to send the email.")