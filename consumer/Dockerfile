FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends build-essential librdkafka-dev libpq-dev ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY consumer.py /app/consumer.py
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

ENV BOOTSTRAP_SERVERS=kafka:9092
ENV POSTGRES_DSN=postgresql://postgres:postgres@timescaledb:5432/logsdb

CMD ["python", "consumer.py"]
