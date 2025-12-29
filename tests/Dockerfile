FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt /tmp/test-requirements.txt
RUN pip install --no-cache-dir -r /tmp/test-requirements.txt

COPY test_integration.py .
CMD ["pytest", "-v", "test_integration.py"]
