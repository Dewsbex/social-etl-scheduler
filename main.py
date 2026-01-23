import os
import threading
import time
import schedule
from flask import Flask, render_template, jsonify
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = Flask(__name__)

# ETL Status Global State
etl_status = {
    "status": "IDLE",
    "last_run": None,
    "logs": []
}

def log_message(message):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}"
    print(log_entry)
    etl_status["logs"].insert(0, log_entry)
    # Keep only last 50 logs
    if len(etl_status["logs"]) > 50:
        etl_status["logs"].pop()

def run_etl_job():
    log_message("Starting ETL Job...")
    etl_status["status"] = "RUNNING"
    
    try:
        # Import and call actual ETL pipeline here
        from etl_pipeline import run_pipeline
        run_pipeline(log_callback=log_message)
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

if __name__ == "__main__":
    # Start Scheduler in a separate thread
    scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True)
    scheduler_thread.start()
    
    # Start Flask Server
    app.run(debug=True, port=5000)
