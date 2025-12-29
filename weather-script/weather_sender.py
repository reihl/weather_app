import os
import requests
import json
import time
import sys
from datetime import datetime, timezone

API_KEY = os.getenv('OPENWEATHER_API_KEY')
RABBIT_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBIT_PASS = os.getenv('RABBITMQ_PASS', 'guest')
RABBIT_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
QUEUE_NAME = 'weather_queue'

RABBIT_API_URL = f"http://{RABBIT_HOST}:15672/api"

def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

if not API_KEY:
    log("Error: OPENWEATHER_API_KEY is missing!")
    sys.exit(1)

def ensure_queue_exists():
    url = f"{RABBIT_API_URL}/queues/%2f/{QUEUE_NAME}"
    payload = {"durable": True, "auto_delete": False, "arguments": {}}
    try:
        requests.put(url, json=payload, auth=(RABBIT_USER, RABBIT_PASS)).raise_for_status()
        log(f"Queue '{QUEUE_NAME}' check passed.")
    except Exception as e:
        log(f"Warning: Could not check queue. RabbitMQ might be initializing. Error: {e}")

def run_task():
    log("Fetching weather data...")
    weather_url = f"https://api.openweathermap.org/data/2.5/weather?q=Paris&appid={API_KEY}&units=metric"
    resp = requests.get(weather_url)
    resp.raise_for_status()
    raw_weather = resp.json().get('main')

    timestamp = datetime.now(timezone.utc).isoformat()
    
    final_message = {
        "timestamp": timestamp,
        "data": raw_weather
    }

    log(f"Sending payload: {json.dumps(final_message)}")
    publish_url = f"{RABBIT_API_URL}/exchanges/%2f/amq.default/publish"
    
    body = {
        "properties": {},
        "routing_key": QUEUE_NAME,
        "payload": json.dumps(final_message),
        "payload_encoding": "string"
    }
    
    requests.post(publish_url, json=body, auth=(RABBIT_USER, RABBIT_PASS)).raise_for_status()
    log("✅ Data successfully sent.")

if __name__ == "__main__":
    log("Starting Weather Bot Service...")
    
    while True:
        try:
            ensure_queue_exists()
            run_task()
        except Exception as e:
            log(f"❌ An error occurred: {e}")
        
        log("Sleeping for 1 hour...")
        time.sleep(3600)
