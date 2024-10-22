import streamlit as st
import requests
import json
import re

# Access API keys from st.secrets
HUBSPOT_API_TOKEN = st.secrets["hubspot_api_token"]

# Set up headers for HubSpot API
headers = {
    "Authorization": f"Bearer {HUBSPOT_API_TOKEN}",
    "Content-Type": "application/json"
}

def extract_file_id(drive_link):
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

def get_all_companies():
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

# --- Streamlit App ---

st.title("Link Google Doc to HubSpot Engagement")

drive_link = st.text_input('Enter the Google Drive or Google Docs link to the document')

if drive_link:
    file_id = extract_file_id(drive_link)
    if file_id:
        # Validate the link (optional)
        st.success("Google Drive link is valid.")
        
        # --- Fetch Companies and Contacts ---
        if 'companies_data' not in st.session_state:
            with st.spinner('Fetching companies...'):
                st.session_state['companies_data'] = get_all_companies()
        if 'contacts_data' not in st.session_state:
            with st.spinner('Fetching contacts...'):
                st.session_state['contacts_data'] = get_all_contacts()
        
        companies_data = st.session_state['companies_data']
        contacts_data = st.session_state['contacts_data']
        
        company_options = {
            company.get('properties', {}).get('name', 'Unnamed Company'): company.get('id')
            for company in companies_data
        }
        contact_options = {
            f"{contact.get('properties', {}).get('firstname', '')} {contact.get('properties', {}).get('lastname', '')}".strip() or "Unnamed Contact": contact.get('id')
            for contact in contacts_data
        }
        
        selected_companies = st.multiselect('Tag Companies', options=list(company_options.keys()))
        selected_contacts = st.multiselect('Tag Contacts', options=list(contact_options.keys()))
        
        # --- Notes ---
        notes = st.text_area('Enter your notes here')
        
        # --- Submit ---
        if st.button('Submit'):
            company_ids = [company_options[name] for name in selected_companies]
            contact_ids = [contact_options[name] for name in selected_contacts]
            
            # Create engagement and associate
            note_body = f"{notes}\n\nGoogle Drive Link: {drive_link}"
            with st.spinner('Creating engagement...'):
                engagement_response = create_engagement(note_body, company_ids, contact_ids)
            
            if engagement_response and 'engagement' in engagement_response:
                st.success("Link and notes have been successfully uploaded to HubSpot.")
            else:
                st.error("An error occurred while creating the engagement.")
    else:
        st.error("Invalid Google Drive or Google Docs link.")