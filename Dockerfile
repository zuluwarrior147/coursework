FROM python:3.10-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only essential application files
COPY scripts/ ./scripts/
COPY pipelines/ ./pipelines/
COPY pyproject.toml .

# Set the Python path to include the current directory
ENV PYTHONPATH=/app

CMD ["python", "-m", "scripts.cli"]