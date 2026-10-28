FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY protofiles ./protofiles
COPY server ./server
COPY client ./client
COPY scripts ./scripts

ENV PYTHONUNBUFFERED=1 \
    PG_CERT_DIR=/certs \
    PG_DATA_DIR=/data
WORKDIR /app/server
