import os
import threading
import time
import schedule
import base64
import uuid
from flask import Flask, render_template, jsonify, request
from dotenv import load_dotenv
from googleapiclient.discovery import build
from etl_pipeline import load_to_calendar, get_credentials, CALENDAR_ID

# Load environment variables
load_dotenv()

app = Flask(__name__)
app.config['PROPAGATE_EXCEPTIONS'] = True
app.config['TRAP_HTTP_EXCEPTIONS'] = True

# ETL Status Global State
etl_status = {
    "status": "IDLE",
    "last_run": None,
    "logs": [],
    "events": [], # Accepted/History events
    "pending_events": [] # Queue for approval
}

def setup_credentials():
    """
    Decodes GOOGLE_CREDENTIALS_BASE64 env var to credentials.json if it exists.
    Useful for Render deployment where we can't upload sensitive files easily.
    """
    encoded_creds = os.environ.get("GOOGLE_CREDENTIALS_BASE64")
    if encoded_creds:
        try:
            print("Found GOOGLE_CREDENTIALS_BASE64, decoding to credentials.json...")
            decoded_bytes = base64.b64decode(encoded_creds)
            with open("credentials.json", "wb") as f:
                f.write(decoded_bytes)
            print("Successfully created credentials.json")
        except Exception as e:
            print(f"Error decoding credentials: {e}")

# Run setup on import
setup_credentials()

def log_message(message):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}"
    print(log_entry)
    etl_status["logs"].insert(0, log_entry)
    # Keep only last 50 logs
    if len(etl_status["logs"]) > 50:
        etl_status["logs"].pop()

def event_callback(event_data):
    """Callback to store found events in the global state."""
    log_message(f"Event Found: {event_data.get('summary', 'Unknown')}")
    # Add timestamp and ID
    event_data['_discovered_at'] = time.strftime("%Y-%m-%d %H:%M:%S")
    if 'id' not in event_data:
        event_data['id'] = str(uuid.uuid4())
    
    # Add to pending queue uniquely
    # Check if duplicate by ID (or Summary+Start?)
    # Simple ID check:
    if not any(e['id'] == event_data['id'] for e in etl_status["pending_events"]):
        etl_status["pending_events"].insert(0, event_data)
    
    # Also keep a history log in "events" but mark as "Pending"
    etl_status["events"].insert(0, {**event_data, "status_tag": "PENDING"})
    
    # Keep only last 50 events
    if len(etl_status["events"]) > 50:
        etl_status["events"].pop()

def run_etl_job():
    log_message("Starting ETL Job...")
    etl_status["status"] = "RUNNING"
    
    try:
        # Import and call actual ETL pipeline here
        from etl_pipeline import run_pipeline
        run_pipeline(log_callback=log_message, event_callback=event_callback)
        # time.sleep(2) # Simulating work - Removed
        # log_message("ETL Job Completed Successfully.") - Logic handled in pipeline or can add here
    except Exception as e:
        log_message(f"ETL Job Failed: {str(e)}")
    finally:
        etl_status["status"] = "IDLE"
        etl_status["last_run"] = time.strftime("%Y-%m-%d %H:%M:%S")

def scheduler_loop():
    # Run once a day at 08:30 AM
    schedule.every().day.at("08:30").do(run_etl_job)
    
    # Also run once heavily at startup? Or wait for manual trigger?
    # schedule.run_all()
    
    while True:
        schedule.run_pending()
        time.sleep(1)

@app.route('/')
def index():
    return render_template('index.html') 

@app.route('/debug')
def debug():
    return render_template('debug.html')

@app.route('/api/status')
def get_status():
    return jsonify(etl_status)

@app.route('/api/trigger', methods=['POST'])
def trigger_etl():
    if etl_status["status"] == "IDLE":
        threading.Thread(target=run_etl_job).start()
        return jsonify({"message": "ETL Job Triggered"}), 200
    else:
        return jsonify({"message": "ETL Job already running"}), 409

@app.route('/api/events/pending', methods=['GET'])
def get_pending():
    return jsonify(etl_status["pending_events"])

@app.route('/api/events/approve', methods=['POST'])
def approve_event():
    event_id = request.json.get('id')
    event_to_approve = next((e for e in etl_status["pending_events"] if e['id'] == event_id), None)
    
    if not event_to_approve:
        return jsonify({"message": "Event not found"}), 404
        
    # Valid Event found in Pending. Now Execute Real Load.
    try:
        # Re-construct service here (or keep a global singleton if thread-safe)
        creds = get_credentials()
        calendar_service = build('calendar', 'v3', credentials=creds)
        
        # We need to transform the event dict back to what load_to_calendar expects IF it expects the *source* JSON. 
        # But wait, load_to_calendar constructs the event body.
        # Actually our `pending_event` IS the Google Calendar body structure (mostly) plus metadata!
        # Because we modified `load_to_calendar` to return the `event` dict.
        # So we can just insert it directly.
        
        # Clean metadata
        body = {k:v for k,v in event_to_approve.items() if k not in ['id', 'source', '_discovered_at', 'status_tag']}
        
        result = calendar_service.events().insert(calendarId=CALENDAR_ID, body=body).execute()
        
        log_message(f"APPROVED & CREATED: {result.get('htmlLink')}")
        
        # Remove from Pending
        etl_status["pending_events"] = [e for e in etl_status["pending_events"] if e['id'] != event_id]
        
        # Update History Status
        for e in etl_status["events"]:
            if e.get('id') == event_id:
                e['status_tag'] = "APPROVED"
                
        return jsonify({"message": "Event Approved", "link": result.get('htmlLink')}), 200
        
    except Exception as e:
        log_message(f"Approval Failed: {e}")
        return jsonify({"message": f"Error: {e}"}), 500

@app.route('/api/events/reject', methods=['POST'])
def reject_event():
    event_id = request.json.get('id')
    etl_status["pending_events"] = [e for e in etl_status["pending_events"] if e['id'] != event_id]
    log_message(f"Event ID {event_id} REJECTED.")
     # Update History Status
    for e in etl_status["events"]:
        if e.get('id') == event_id:
            e['status_tag'] = "REJECTED"
            
    return jsonify({"message": "Event Rejected"}), 200

# Start Scheduler in a separate thread (Works for Gunicorn worker too)
# Note: In a production environment with multiple workers, this would start a scheduler for EACH worker.
# Ideally, use a separate worker or process for the scheduler, but for this simple app, one worker or just running it is fine.
# We can use a simple lock or just let it run (duplicate checks might be needed if scaling up).
if not os.environ.get("WERKZEUG_RUN_MAIN"): # Avoid running twice during Flask debug reload 
    scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True)
    scheduler_thread.start()

if __name__ == "__main__":
    # Start Flask Server
    app.run(debug=True, port=5000)
