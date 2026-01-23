import os
import datetime
import base64
import json
import time
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import google.generativeai as genai
from heuristics import identify_child, check_gift_heuristic, check_costume_heuristic

# Scopes required for the application
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/calendar.events'
]

def get_credentials():
    """Gets valid user credentials from storage or initiates OAuth flow."""
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists('credentials.json'):
                raise FileNotFoundError("credentials.json not found. Please add GCP credentials.")
                
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
            
    return creds

def extract_emails(service, query="label:inbox -category:promotions -category:social"):
    """
    Phase 1: EXTRACT
    """
    # Augment query with project specific keywords to be safe, or rely on generalized "school" filter?
    # For now, let's stick to the brief's example triggers if possible, or broad inbox + analysis
    # Brief says: "New email from specific domains... OR emails containing keywords"
    
    keywords = ["Trip", "Assembly", "Birthday", "Party", "Costume", "Bring", "Year 3", "Reception"]
    keyword_query = " OR ".join([f'"{k}"' for k in keywords])
    full_query = f"{query} ({keyword_query})"
    
    # We only want *new* emails usually, but for this demo/MVP we might scan recent X
    # In a real poller, we'd store specific history ID. For now, let's grab last 10 relevant messages.
    results = service.users().messages().list(userId='me', q=full_query, maxResults=5).execute()
    messages = results.get('messages', [])
    
    email_data_list = []
    
    for msg in messages:
        txt = service.users().messages().get(userId='me', id=msg['id']).execute()
        payload = txt['payload']
        headers = payload['headers']
        
        subject = next((h['value'] for h in headers if h['name'] == 'Subject'), "No Subject")
        sender = next((h['value'] for h in headers if h['name'] == 'From'), "Unknown")
        
        # Simple body extraction (multipart handling can be complex, simplifying for MVP)
        body = ""
        if 'parts' in payload:
            for part in payload['parts']:
                if part['mimeType'] == 'text/plain':
                    data = part['body'].get('data')
                    if data:
                        body += base64.urlsafe_b64decode(data).decode()
        elif 'body' in payload:
            data = payload['body'].get('data')
            if data:
                body += base64.urlsafe_b64decode(data).decode()
                
        email_data_list.append({
            "id": msg['id'],
            "subject": subject,
            "sender": sender,
            "body": body
        })
        
    return email_data_list

def transform_email_content(email_data):
    """
    Phase 2: TRANSFORM with Gemini 1.5 Pro
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("Error: GEMINI_API_KEY not set.")
        return None
        
    genai.configure(api_key=api_key)
    
    # Using gemini-1.5-flash for speed/cost in demo, Pro is requested but Flash is often sufficient for text extraction
    # Switching to provided model req: Gemini 1.5 Pro
    model = genai.GenerativeModel('gemini-1.5-pro')
    
    prompt = f"""
    You are an executive assistant extracting structured JSON from messy school emails. 
    You must distinguish between Benjamin (Year 2/Reception age approx) and Tristan (Year 3 age approx).
    
    Email Subject: {email_data['subject']}
    Email Body:
    {email_data['body']}
    
    Extract the following JSON structure ONLY. If no event is found, return null.
    {{
        "event_title": "Short title",
        "start_time": "ISO 8601 format (YYYY-MM-DDTHH:MM:SS)",
        "end_time": "ISO 8601 format",
        "location": "Location string",
        "description": "Details including 'To Do' items like 'Bring packed lunch', costs, etc.",
        "subjects": ["Benjamin", "Tristan"] (List of detected children based on Year/Name)
    }}
    
    Heuristics to apply:
    - Look for "Year 3" for Tristan.
    - Look for "Reception" or "Year 2" for Benjamin.
    """
    
    try:
        response = model.generate_content(prompt)
        text = response.text
        # Clean markdown if present
        if "```json" in text:
            text = text.replace("```json", "").replace("```", "")
        
        event_json = json.loads(text)
        return event_json
    except Exception as e:
        print(f"Transformation failed: {e}")
        return None

def load_to_calendar(service, event_json, dry_run=False):
    """
    Phase 3: LOAD
    """
    if not event_json: 
        return "No detected event data."
        
    # Apply Python Heuristics (Post-LLM refinement)
    # Rule 1: "Who" - The LLM does this, but we can double check or augment via title
    # (LLM returns 'subjects', we can tag them in title)
    
    subjects = event_json.get("subjects", [])
    # Fallback if LLM missed it but heuristic finds it
    if not subjects:
        # We assume the body content passed to heuristic is separate, 
        # but here we only have JSON. 
        # Let's rely on what we have.
        title_tag = ""
    else:
        title_tag = f"[{', '.join(subjects)}]"

    final_title = f"{title_tag} {event_json.get('event_title', 'School Event')}"
    
    # Rule 2: Gift Heuristic
    description = event_json.get("description", "")
    if check_gift_heuristic(event_json.get("event_title", ""), description):
        description = "🎁 REMINDER: BUY GIFT! \n\n" + description
        
    # Rule 3: Costume Protocol
    color_id = "1" # Lavender default
    # If we had the original text here we could run check_costume_heuristic(original_text)
    # For now let's check description/title
    if check_costume_heuristic(final_title + " " + description):
        final_title = "⚠️ COSTUME: " + final_title
        color_id = "11" # Red
        
    event = {
        'summary': final_title,
        'location': event_json.get('location', ''),
        'description': description,
        'start': {
            'dateTime': event_json.get('start_time'), # strict ISO 8601 coming from Gemini
            'timeZone': 'Europe/London', # Assuming UK based on "Mufti", "Year 2", "£"
        },
        'end': {
            'dateTime': event_json.get('end_time'),
            'timeZone': 'Europe/London',
        },
        'colorId': color_id,
        'status': 'tentative', # As requested: Grey/Tentative usually means 'tentative' in API or guestsCanModify?
        # Actually Google Calendar 'status' can be 'confirmed', 'tentative', 'cancelled'. 
        # Visually 'tentative' might look different (hatched).
    }
    
    if dry_run:
        return f"[DRY RUN] Would create: {final_title} at {event_json.get('start_time')}"
        
    try:
        event_result = service.events().insert(calendarId='primary', body=event).execute()
        return f"Event created: {event_result.get('htmlLink')}"
    except Exception as e:
        return f"Calendar Insert Failed: {e}"

def run_pipeline(log_callback=print, event_callback=None):
    log_callback("Initializing ETL Pipeline...")
    
    try:
        creds = get_credentials()
        gmail_service = build('gmail', 'v1', credentials=creds)
        calendar_service = build('calendar', 'v3', credentials=creds)
        
        log_callback("Authenticating: SUCCESS")
    except Exception as e:
        log_callback(f"Authentication Failed: {e}")
        return

    log_callback("Phase 1: Scanning Inbox...")
    emails = extract_emails(gmail_service)
    
    if not emails:
        log_callback("No relevant recent emails found.")
        return

    log_callback(f"Found {len(emails)} candidate emails.")
    
    for email in emails:
        log_callback(f"Processing: {email['subject']}...")
        
        # Transform
        event_data = transform_email_content(email)
        
        if event_data:
            log_callback(f"Transform SUCCESS: {json.dumps(event_data, indent=2)}")
            
            # Load
            result = load_to_calendar(calendar_service, event_data)
            log_callback(f"Load Result: {result}")
            
            if event_callback:
                event_callback(event_data)
        else:
            log_callback("Transform: No event detected in email.")
            
    log_callback("ETL Job Finished.")

if __name__ == "__main__":
    # Local test
    run_pipeline()
