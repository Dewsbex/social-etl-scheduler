import json
import os
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, "pipeline_state.json")
CONFIG_FILE = os.path.join(BASE_DIR, "config.json")

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
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_config(config_data):
    """Saves configuration to config.json."""
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config_data, f, indent=4)
        return True
    except Exception:
        return False
