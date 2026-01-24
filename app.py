import os
import threading
import time
import schedule
import base64
from flask import Flask, render_template, jsonify
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = Flask(__name__)

# ETL Status Global State
etl_status = {
    "status": "IDLE",
    "last_run": None,
    "logs": [],
    "events": []
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
    log_message(f"Event Found: {event_data.get('event_title', 'Unknown')}")
    # Add timestamp
    event_data['_discovered_at'] = time.strftime("%Y-%m-%d %H:%M:%S")
    etl_status["events"].insert(0, event_data)
    # Keep only last 20 events
    if len(etl_status["events"]) > 20:
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
    # Run every 6 hours
    schedule.every(6).hours.do(run_etl_job)
    
    # Also run once heavily at startup? Or wait for manual trigger?
    # schedule.run_all()
    
    while True:
        schedule.run_pending()
        time.sleep(1)

@app.route('/')
def index():
    return render_template('index.html') # We will need to put index.html in templates/ or configure static folder

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
