FROM astrocrpublic.azurecr.io/runtime:3.1-12

USER root

# Copy requirements.txt to a non-DAG directory
COPY requirements.txt /requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Switch back to the default Airflow user
USER airflow

COPY . /usr/local/airflow
RUN pip install -e .
