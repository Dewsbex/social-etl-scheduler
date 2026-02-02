import json
import os
import time
import shutil
import requests

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
CONFIG_TEMPLATE = os.path.join(BASE_DIR, "config.template.json")

# GitHub Gist configuration
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
CONFIG_GIST_ID = os.getenv("CONFIG_GIST_ID")

def load_template_config():
    """Load config from template file as fallback."""
    if os.path.exists(CONFIG_TEMPLATE):
        try:
            with open(CONFIG_TEMPLATE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading template: {e}")
            return {}
    return {}

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
    """Load config from GitHub Gist (or template as fallback)."""
    # If GitHub credentials not configured, use template
    if not GITHUB_TOKEN or not CONFIG_GIST_ID:
        print("GitHub Gist not configured, using template")
        return load_template_config()
    
    try:
        headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json"
        }
        url = f"https://api.github.com/gists/{CONFIG_GIST_ID}"
        
        print(f"Loading config from Gist: {CONFIG_GIST_ID}")
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        
        gist_data = r.json()
        
        # Check if config.json exists in the gist
        if "config.json" not in gist_data.get("files", {}):
            print("config.json not found in Gist, using template")
            return load_template_config()
        
        config_content = gist_data["files"]["config.json"]["content"]
        config = json.loads(config_content)
        print("Successfully loaded config from Gist")
        return config
        
    except requests.exceptions.RequestException as e:
        print(f"Error loading from Gist (network): {e}")
        return load_template_config()
    except Exception as e:
        print(f"Error loading from Gist: {e}")
        return load_template_config()

def save_config(config_data):
    """Save config to GitHub Gist."""
    # If GitHub credentials not configured, cannot save
    if not GITHUB_TOKEN or not CONFIG_GIST_ID:
        print("GitHub Gist not configured, cannot save")
        return False
    
    try:
        headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json"
        }
        url = f"https://api.github.com/gists/{CONFIG_GIST_ID}"
        
        payload = {
            "files": {
                "config.json": {
                    "content": json.dumps(config_data, indent=4)
                }
            }
        }
        
        print(f"Saving config to Gist: {CONFIG_GIST_ID}")
        r = requests.patch(url, headers=headers, json=payload, timeout=10)
        r.raise_for_status()
        
        print("Successfully saved config to Gist")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"Error saving to Gist (network): {e}")
        return False
    except Exception as e:
        print(f"Error saving to Gist: {e}")
        return False

