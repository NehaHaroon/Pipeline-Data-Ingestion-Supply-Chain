FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create storage directories
RUN mkdir -p storage/raw storage/ingested storage/quarantine storage/cdc_log storage/checkpoints storage/micro_batch storage/stream_buffer logs

EXPOSE 8000

# Default command: run the API
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]