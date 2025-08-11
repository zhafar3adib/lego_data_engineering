FROM apache/airflow:2.9.1-python3.9

# copy requirements.txt
COPY requirements.txt /opt/airflow/requirements.txt

USER airflow

# Install all dependencies
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
