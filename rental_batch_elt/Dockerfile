FROM python:3.10-slim

# Install dependencies
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    curl \
    && apt-get clean

# Set working directory
WORKDIR /app

# Install DBT (Snowflake adapter)
RUN pip install --no-cache-dir dbt-core dbt-snowflake boto3

# Copy DBT project into container
COPY rental_batch_elt/ /app
COPY get_params.py /app

# Entrypoint (so you can run commands like `dbt run`)
ENTRYPOINT ["sh", "-c", "python get_params.py && dbt debug && dbt run"]