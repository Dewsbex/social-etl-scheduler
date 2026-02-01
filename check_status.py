import requests
import json

url = "http://127.0.0.1:5000/api/status"
try:
    response = requests.get(url)
    data = response.json()
    print("ETL Status:", data["status"])
    print("Latest Logs:")
    for log in data["logs"][:20]: # Show last 20 logs
        print(log)
except Exception as e:
    print(f"Error: {e}")
