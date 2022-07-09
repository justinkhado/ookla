FROM apache/airflow:2.3.2-python3.9

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt