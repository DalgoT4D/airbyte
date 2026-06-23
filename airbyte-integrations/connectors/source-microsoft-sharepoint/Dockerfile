FROM python:3.11-slim

WORKDIR /airbyte/integration_code

# Copy the entire connector directory into the container
COPY . .

# Install poetry and the connector's dependencies directly into the system
RUN pip install --no-cache-dir poetry==1.7.1
RUN poetry config virtualenvs.create false
RUN poetry install --only main

# Define the required Airbyte environment variables and entrypoint
ENV AIRBYTE_ENTRYPOINT "python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]