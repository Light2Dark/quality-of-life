FROM prefecthq/prefect:2.10.2-python3.10
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .

# BUILD TO Artifact registry: docker build -t asia-southeast1-docker.pkg.dev/quality-of-life-364309/prefect-container/prefect-python3.10 .
# PUSH to AR: docker push asia-southeast1-docker.pkg.dev/quality-of-life-364309/prefect-container/prefect-python3.10