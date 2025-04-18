# Use Python 3.10 slim image
FROM python:3.10-slim

# Set environment variable to prevent .pyc files and buffering issues
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /appS

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your code
COPY . .

# Default command
CMD ["python", "data_generator_s3.py"]
