FROM python:3.11-slim

# install libs required by confluent-kafka / psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential librdkafka-dev libpq-dev ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app.py /app/app.py

EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
