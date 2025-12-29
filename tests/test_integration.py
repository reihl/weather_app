import os
import requests
import pytest
import time

RABBIT_HOST = "rabbitmq"
RABBIT_API = f"http://{RABBIT_HOST}:15672/api/overview"

def test_rabbitmq_connection():
    """Wait for RabbitMQ to be ready and check connection"""
    user = os.getenv('RABBITMQ_USER', 'guest')
    password = os.getenv('RABBITMQ_PASS', 'guest')
    
    for i in range(10):
        try:
            resp = requests.get(RABBIT_API, auth=(user, password))
            if resp.status_code == 200:
                print("✅ Connected to RabbitMQ!")
                assert True
                return
        except requests.exceptions.ConnectionError:
            print(f"Waiting for RabbitMQ... ({i+1}/10)")
            time.sleep(5)
            
    pytest.fail("❌ Could not connect to RabbitMQ after 50 seconds")
