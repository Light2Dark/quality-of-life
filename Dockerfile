FROM prefecthq/prefect:2.10.2-python3.10
COPY requirements.txt .
RUN pip install -r requirements.txt