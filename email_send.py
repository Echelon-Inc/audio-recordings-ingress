from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow 
from email.mime.text import MIMEText
import base64
import os

# Define the SCOPES
SCOPES = ['https://www.googleapis.com/auth/gmail.send']

# Function to create an email message
def create_message(sender, to, subject, message_text):
    message = MIMEText(message_text)
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode('utf-8')
    return {'raw': raw}

# Send the email using Gmail API
def send_email(service, user_id, message):
    service.users().messages().send(userId=user_id, body=message).execute()

# Example usage
creds = None
if os.path.exists('token.json'):
    creds = Credentials.from_authorized_user_file('token.json', SCOPES)
else:
    flow = InstalledAppFlow.from_client_secrets_file(
        'email_credentials.json', SCOPES)
    creds = flow.run_local_server(port=0)
    with open('token.json', 'w') as token:
        token.write(creds.to_json())

service = build('gmail', 'v1', credentials=creds)

message = create_message("christian@echelon.xyz", "christian@echelon.xyz", "Transcription Review Required", "Please review the transcription.")
send_email(service, "me", message)