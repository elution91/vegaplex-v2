# syntax=docker/dockerfile:1.6
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# System deps for scientific Python wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgomp1 \
 && rm -rf /var/lib/apt/lists/*

# Install Python deps first for layer caching
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy application code
COPY . /app/

# Render injects $PORT; default to 8000 for local docker
ENV PORT=8000
EXPOSE 8000

RUN chmod +x /app/entrypoint.sh
CMD ["/app/entrypoint.sh"]
