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
from heuristics import identify_child, check_gift_heuristic, check_costume_heuristic, heuristic_extraction
from portal_scanner import scan_school_portal
from state_manager import get_last_successful_run, update_last_successful_run, load_config
import asyncio
from datetime import datetime
import math


# Scopes required for the application
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/calendar.events'
]

# Configuration
CALENDAR_ID = os.getenv('GOOGLE_CALENDAR_ID', '9k5kqvc6322s3ro121soijjc6g@group.calendar.google.com')

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

def extract_emails(service, query="label:inbox", date_filter="newer_than:1d"):
    """
    Phase 1: EXTRACT
    """
    config = load_config()
    search_settings = config.get("search_settings", {})
    filtering_logic = config.get("filtering_logic", {})
    
    # Flatten all search terms
    all_terms = []
    all_terms.extend(search_settings.get("children", []))
    all_terms.extend(search_settings.get("schools", []))
    all_terms.extend(search_settings.get("clubs", []))
    all_terms.extend(search_settings.get("general_keywords", []))
    
    # Deduplicate and quote
    if not all_terms:
         # Fallback to hardcoded defaults if config is broken
         all_terms = [
            "School Trip", "Assembly", "Sports Day", "Parent Evening", "PTA", "Costume Day", 
            "Year 3", "Year 5", "Year 6", "Reception Year", "Wednesday Notice",
            "Benjamin Dewsbery", "Benji Dewsbery", "Tristan Dewsbery",
            "Bishop Gilpin", "Dees Days", "FOBG", "Friends of Bishop Gilpin",
            "Krispy Kreme", "donut", "fundraiser"
        ]
    
    # Construct OR query for terms
    terms_query = " OR ".join([f'"{t}"' for t in all_terms])
    
    # Exclusions
    exclusions = filtering_logic.get("exclude_keywords", ["MARC", "SADIQ", "ENERGY"])
    # Removed "NEWSLETTER" to allow AI to parse newsletters for dates
    exclusion_query = " ".join([f"-{e}" for e in exclusions])
    
    # Filter for emails based on dynamic date filter
    # ULTRA-STRICT: Only precise school entities + Exclude Noise
    full_query = f"{query} ({terms_query}) {date_filter} {exclusion_query}"
    
    # Increased to 500 for historical backfill
    results = service.users().messages().list(userId='me', q=full_query, maxResults=500).execute()
    messages = results.get('messages', [])
    
    email_data_list = []
    
    for msg in messages:
        txt = service.users().messages().get(userId='me', id=msg['id']).execute()
        payload = txt['payload']
        headers = payload['headers']
        
        subject = next((h['value'] for h in headers if h['name'] == 'Subject'), "No Subject")
        sender = next((h['value'] for h in headers if h['name'] == 'From'), "Unknown")
        
        # Body extraction - handle both plain text and HTML
        plain_text = ""
        html_content = ""
        
        def walk_parts(parts):
            nonlocal plain_text, html_content
            for part in parts:
                mime = part.get('mimeType')
                data = part.get('body', {}).get('data')
                
                if mime == 'text/plain' and data:
                    plain_text += base64.urlsafe_b64decode(data).decode()
                elif mime == 'text/html' and data:
                    html_content += base64.urlsafe_b64decode(data).decode()
                elif 'parts' in part:
                    walk_parts(part['parts'])

        if 'parts' in payload:
            walk_parts(payload['parts'])
        elif 'body' in payload:
            data = payload['body'].get('data')
            if data:
                body_str = base64.urlsafe_b64decode(data).decode()
                if payload.get('mimeType') == 'text/html':
                    html_content = body_str
                else:
                    plain_text = body_str

        # Decision: If HTML is present, it's usually the "richer" source for school notices
        # We append both to be safe, or just use the largest one
        body = html_content if len(html_content) > len(plain_text) else plain_text
                
        email_data_list.append({
            "id": msg['id'],
            "subject": subject,
            "sender": sender,
            "body": body
        })
        
    return email_data_list

import re

def strip_html(html_str):
    """Simple regex based HTML tag stripper."""
    if not html_str: return ""
    clean = re.compile('<.*?>')
    return re.sub(clean, ' ', html_str)

def transform_email_content(email_data, log_callback=print):
    """
    Phase 2: TRANSFORM with Gemini 1.5 Pro
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        log_callback("Error: GEMINI_API_KEY not set.")
        return None
        
    genai.configure(api_key=api_key)
    
    # Strip HTML for cleaner extraction
    body_clean = strip_html(email_data.get('body', ''))
    
    # Load configuration for dynamic prompting
    config = load_config()
    search_settings = config.get("search_settings", {})
    children = search_settings.get("children", ["Benjamin Dewsbery", "Tristan Dewsbery"])
    keywords = search_settings.get("general_keywords", [])
    years = search_settings.get("year_groups", [])

    prompt = f"""
    You are a Logistics Officer. Your goal is to extract calendar events/deadlines from school emails.
    
    Email Subject: {email_data.get('subject', 'No Subject')}
    Email Body:
    {body_clean[:4000]}
    
    Contextual Targets:
    - Children: {', '.join(children)}
    - Keywords: {', '.join(keywords)}
    - Year Groups: {', '.join(years)}
    
    Instructions:
    - Look for dates in SUBJECT and Body.
    - IGNORE emails that are just "Newsletters", "Weekly Updates", or notifications that a document has been "Released" or is "Available" unless they contain a specific future event date or deadline.
    - EXTRACT EVERY REAL EVENT (Trips, Early Closures, Sales, Deadlines, Medical).
    - Even extract past events (Nov 2025) for testing.
    - If year is missing: assume 2026 if date is in Jan-Aug, or 2025 if it's Nov-Dec.
    - Distinguish between children based on their name or associated Year Group.
    - Handle double-date formats like "11/03/2026/11/03/2026" by taking the first part.
    
    Return a JSON object with:
    1. "found": boolean
    2. "analysis": "One sentence explaining why it is or isn't an event"
    3. "event": {{ ... }} or null
    
    Event template:
    {{
        "event_title": "Descriptive title",
        "start_time": "YYYY-MM-DDTHH:MM:SS",
        "end_time": "YYYY-MM-DDTHH:MM:SS",
        "location": "Bishop Gilpin / School",
        "description": "Details...",
        "subjects": ["Child Name 1", "Child Name 2"]
    }}
    """
    
    # Try multiple model names for better compatibility
    response = None
    # Verified options in this environment: models/gemini-2.5-flash, models/gemini-2.0-flash, models/gemini-flash-latest
    for model_name in ['models/gemini-2.5-flash', 'models/gemini-2.0-flash', 'models/gemini-flash-latest', 'models/gemini-pro-latest']:
        try:
            print(f"Logistics Brain: Attempting analysis with {model_name}...")
            model = genai.GenerativeModel(model_name)
            response = model.generate_content(prompt)
            if response:
                # Store which model succeeded in the return message
                used_model = model_name
                break
        except Exception as e:
            last_err = str(e)
            print(f"Logistics Brain: {model_name} failed: {last_err}")
            continue
    else:
        return None, f"All Gemini models failed. Last Error: {last_err}"
    
    try:
        text = response.text.strip()
        # Clean markdown
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()
            
        res_json = json.loads(text)
        analysis = res_json.get("analysis", "No analysis provided.")
        
        if res_json.get("found") and res_json.get("event"):
            return res_json["event"], analysis
        else:
            return None, analysis
            
    except Exception as e:
        print(f"Transformation failed: {e}")
        return None, f"Analysis Failed: {e}"

def check_calendar_conflicts(service, start_time, end_time):
    """
    Checks for existing events in the given time range.
    Returns list of conflicting event summaries.
    """
    try:
        events_result = service.events().list(
            calendarId=CALENDAR_ID, 
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

def load_to_calendar(service, event_json, dry_run=False, approval_mode=False, raw_body=None):
    """
    Phase 3: LOAD
    """
    # Post-LLM Refinement: Apply the User's strict labeling heuristics
    # We combine Subject (Event Title) and Body for the most accurate labeling
    title = event_json.get('event_title', '')
    matching_text = f"{title} {raw_body}" if raw_body else f"{title} {event_json.get('description', '')}"
    subjects = identify_child(matching_text)
    
    if subjects == "IGNORE":
        return "Skipped: Irrelevant Year Group", None
        
    if not subjects:
        title_tag = "[Bishop Gilpin]"
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
        # color_id = "11" # Optional: Make red on conflict

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
        'source_url': event_json.get('gmail_url') or event_json.get('source_url')
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
        event_result = service.events().insert(calendarId=CALENDAR_ID, body=event).execute()
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
    
    # Determine lookback period based on state
    last_run_ts = get_last_successful_run()
    if last_run_ts:
        days_since = (time.time() - last_run_ts) / (24 * 3600)
        # Add 1 day buffer to be safe
        lookback_days = math.ceil(days_since) + 1
        date_filter = f"newer_than:{lookback_days}d"
        log_callback(f" > Last success: {datetime.fromtimestamp(last_run_ts).strftime('%Y-%m-%d %H:%M')}. Scanning last {lookback_days} days.")
    else:
        # Initial run / fallback
        date_filter = "newer_than:6m"
        log_callback(" > No previous state found. Running INITIAL 6-MONTH BACKFILL.")
        
    emails = extract_emails(gmail_service, date_filter=date_filter)
    
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
            # Pre-AI Filter: Check heuristics first to save tokens
            full_text = f"{email['subject']} {email['body']}"
            pre_subjects = identify_child(full_text)
            
            if pre_subjects == "IGNORE":
                log_callback(f"Skipping (Heuristic Ignore): {email['subject']}...")
                continue
                
            log_callback(f"Processing: {email['subject']}... <a href='https://mail.google.com/mail/u/0/#inbox/{email['id']}' target='_blank' style='color:#00ffff; text-decoration:none;'>[ SOURCE ]</a>")
            event_data = heuristic_extraction(email.get('body', ''), email.get('subject', ''), email['id'])
            if event_data:
                event_data['source'] = 'email' # Tag source
                log_callback(f"   > Date Extracted: {event_data['start_time'][:10]}")
                
                # Load (Approval Mode = True for Vibe Lab Logistics)
                result_msg, pending_event = load_to_calendar(calendar_service, event_data, approval_mode=True, raw_body=email.get('body'))
                log_callback(f" > {result_msg}")
                
                # If approval_mode is True, send to Logistics Module via callback
                if pending_event and event_callback:
                    event_callback(pending_event)
            
            # Rate limit to avoid 429 quota errors on free tier (15 RPM)
            # Increased to 10s for absolute safety
            time.sleep(10)
    else:
        log_callback("No relevant recent emails found.")

    # 2. Process Portal Events
    if portal_events:
        for p_event in portal_events:
            log_callback(f"Processing Portal Event: {p_event.get('event_title')}...")
            # Portal events are already JSON, proceed to Load
            # Ensure they have required fields
            if 'start_time' in p_event:
                 # Load (Approval Mode = True for Portal Events)
                 result_msg, pending_event = load_to_calendar(calendar_service, p_event, approval_mode=True)
                 log_callback(f" > {result_msg}")
                 
                 # If approval_mode is True, send to Logistics Module via callback
                 if pending_event and event_callback:
                     event_callback(pending_event)
            else:
                log_callback("Skipping invalid portal event data.")

    # Update state only if we reached the end successfully
    update_last_successful_run()
    log_callback("Pipeline Complete. State saved.")

    log_callback("ETL Job Finished.")
            
    log_callback("ETL Job Finished.")

if __name__ == "__main__":
    # Local test
    run_pipeline()
