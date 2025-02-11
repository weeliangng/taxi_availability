FROM apache/airflow:latest
USER airflow
RUN pip install --no-cache-dir poetry
RUN poetry config virtualenvs.create false

# Copy dependency files
WORKDIR /opt/airflow
COPY pyproject.toml poetry.lock ./

# Install dependencies
RUN poetry install --no-root

USER airflow