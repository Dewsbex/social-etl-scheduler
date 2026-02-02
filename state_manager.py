import json
import os
import time
import shutil

# Determine if we're running on Render (persistent disk available)
# On Render, use /var/data for persistent storage
# Locally, use project root
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Check if Render persistent disk is mounted
if os.path.exists('/var/data'):
    PERSISTENT_DIR = '/var/data'
else:
    PERSISTENT_DIR = BASE_DIR

STATE_FILE = os.path.join(PERSISTENT_DIR, "pipeline_state.json")
CONFIG_FILE = os.path.join(PERSISTENT_DIR, "config.json")
CONFIG_TEMPLATE = os.path.join(BASE_DIR, "config.template.json")

def initialize_config():
    """Initialize config.json from template if it doesn't exist."""
    if not os.path.exists(CONFIG_FILE):
        if os.path.exists(CONFIG_TEMPLATE):
            print(f"Initializing config from template: {CONFIG_TEMPLATE} -> {CONFIG_FILE}")
            shutil.copy(CONFIG_TEMPLATE, CONFIG_FILE)
        else:
            print("Warning: No config template found, creating empty config")
            with open(CONFIG_FILE, 'w') as f:
                json.dump({}, f)

def get_last_successful_run():
    """Returns the timestamp of the last successful run, or a default lookback if none exists."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                data = json.load(f)
                return data.get("last_run_timestamp", None)
        except Exception:
            return None
    return None

def update_last_successful_run():
    """Updates the last successful run timestamp to now."""
    with open(STATE_FILE, 'w') as f:
        json.dump({"last_run_timestamp": time.time()}, f)

def load_config():
    """Loads configuration from config.json."""
    initialize_config()  # Ensure config exists
    
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading config: {e}")
            return {}
    return {}

def save_config(config_data):
    """Saves configuration to config.json."""
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
        
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config_data, f, indent=4)
        print(f"Config saved to: {CONFIG_FILE}")
        return True
    except Exception as e:
        print(f"Error saving config: {e}")
        return False

