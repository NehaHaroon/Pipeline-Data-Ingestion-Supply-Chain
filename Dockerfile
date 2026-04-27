FROM python:3.11-slim

# Create non-root user
RUN useradd --create-home --shell /bin/bash app

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y docker.io docker-compose

# Copy application
COPY . .

# Create storage directories and set permissions
RUN mkdir -p storage/raw storage/ingested storage/quarantine storage/cdc_log storage/checkpoints storage/micro_batch storage/stream_buffer logs && \
    chown -R app:app /app
# Switch to non-root user
USER app

EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command: run the production script
CMD ["python", "run_production.py"]