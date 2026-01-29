FROM python:3.11-slim

WORKDIR /dbt

# Install dbt with DuckDB adapter
RUN pip install --no-cache-dir \
    dbt-core==1.7.4 \
    dbt-duckdb==1.7.1

# Copy dbt project files
COPY . .

# Set dbt profiles directory
ENV DBT_PROFILES_DIR=/dbt

# Default command
CMD ["dbt", "run"]
