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
from portal_scanner import scan_school_portal
import asyncio
from datetime import datetime


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
    
    # Filter for emails from the last 24 hours (newer_than:1d)
    full_query = f"{query} {keyword_query} newer_than:1d"
    
    # We only want *new* emails usually, but for this demo/MVP we might scan recent X
    # In a real poller, we'd store specific history ID. For now, let's grab last 20 relevant messages.
    results = service.users().messages().list(userId='me', q=full_query, maxResults=20).execute()
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

def check_calendar_conflicts(service, start_time, end_time):
    """
    Checks for existing events in the given time range.
    Returns list of conflicting event summaries.
    """
    try:
        events_result = service.events().list(
            calendarId='primary', 
            timeMin=start_time, 
            timeMax=end_time,
            singleEvents=True,
            orderBy='startTime'
        ).execute()
        events = events_result.get('items', [])
        return [e['summary'] for e in events]
    except Exception as e:
        print(f"Conflict check failed: {e}")
        return []

def load_to_calendar(service, event_json, dry_run=False, approval_mode=False):
    """
    Phase 3: LOAD
    """
    if not event_json: 
        return "No detected event data.", None
        
    # Apply Python Heuristics (Post-LLM refinement)
    subjects = event_json.get("subjects", [])
    if not subjects:
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
    if check_costume_heuristic(final_title + " " + description):
        final_title = "⚠️ COSTUME: " + final_title
        color_id = "11" # Red
        
    start_time = event_json.get('start_time')
    end_time = event_json.get('end_time')

    # Conflict Check
    conflicts = check_calendar_conflicts(service, start_time, end_time)
    if conflicts:
        conflict_msg = f"\n\nCONFLICTS DETECTED: {', '.join(conflicts)}"
        description += conflict_msg
        final_title = "⚠️ CONFLICT: " + final_title

    event = {
        'summary': final_title,
        'location': event_json.get('location', ''),
        'description': description,
        'start': {
            'dateTime': start_time, 
            'timeZone': 'Europe/London', 
        },
        'end': {
            'dateTime': end_time,
            'timeZone': 'Europe/London',
        },
        'colorId': color_id,
        'status': 'tentative', 
    }
    
    # Logic:
    # If approval_mode is True: DO NOT insert. Return the event dict for the pending queue.
    # If dry_run is True: Print what would happen.
    
    if approval_mode:
        # Return the event object intended for the "Pending" queue
        # We add metadata for the UI
        event['id'] = event_json.get('id', 'generated_' + datetime.now().strftime("%Y%m%d%H%M%S")) # Generate a temp ID if missing
        event['source'] = event_json.get('source', 'email')
        return "Queued for Approval", event

    if dry_run:
        return f"[DRY RUN] Would create: {final_title} at {start_time}", None
        
    try:
        event_result = service.events().insert(calendarId='primary', body=event).execute()
        return f"Event created: {event_result.get('htmlLink')}", None
    except Exception as e:
        return f"Calendar Insert Failed: {e}", None

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
    
    # Phase 1b: Portal Scanning (Async)
    log_callback("Phase 1b: Scanning School Portal...")
    try:
         portal_events = asyncio.run(scan_school_portal())
         if portal_events:
             log_callback(f"Found {len(portal_events)} events from Portal.")
         else:
             log_callback("No events found from Portal.")
    except Exception as e:
        log_callback(f"Portal Scan Failed: {e}")
        portal_events = []

    # Combined Processing
    # We treat extracted emails as 'raw sources' that need transform
    # We treat portal events as 'already transformed' (mostly) but needing calendar loading
    
    # 1. Process Emails
    if emails:
        log_callback(f"Found {len(emails)} candidate emails.")
        for email in emails:
            log_callback(f"Processing Email: {email['subject']}...")
            event_data = transform_email_content(email)
            
            if event_data:
                event_data['source'] = 'email' # Tag source
                log_callback(f"Transform SUCCESS: {json.dumps(event_data, indent=2)}")
                
                # Load (Approval Mode = True by default for Logistics Officer)
                result_msg, pending_event = load_to_calendar(calendar_service, event_data, approval_mode=True)
                log_callback(f"Load Result: {result_msg}")
                
                if pending_event and event_callback:
                    event_callback(pending_event)
            else:
                log_callback("Transform: No event detected in email.")
    else:
        log_callback("No relevant recent emails found.")

    # 2. Process Portal Events
    if portal_events:
        for p_event in portal_events:
            log_callback(f"Processing Portal Event: {p_event.get('event_title')}...")
            # Portal events are already JSON, proceed to Load
            # Ensure they have required fields
            if 'start_time' in p_event:
                 result_msg, pending_event = load_to_calendar(calendar_service, p_event, approval_mode=True)
                 log_callback(f"Load Result: {result_msg}")
                 if pending_event and event_callback:
                    event_callback(pending_event)
            else:
                log_callback("Skipping invalid portal event data.")

    log_callback("ETL Job Finished.")
            
    log_callback("ETL Job Finished.")

if __name__ == "__main__":
    # Local test
    run_pipeline()
