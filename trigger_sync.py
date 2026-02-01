import requests
import time

url = "http://127.0.0.1:5000/api/trigger"
print(f"Triggering sync at {url}...")
try:
    response = requests.post(url)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
except Exception as e:
    print(f"Error: {e}")
