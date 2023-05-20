from pipelines.air_quality import test_elt_flow, elt_flow
from prefect import get_client
from prefect.deployments import Deployment 
from prefect.filesystems import GitHub
from prefect_gcp.cloud_storage import GcsBucket
import os
from dotenv import load_dotenv

load_dotenv()
client = get_client()

# PREFECT DEPLOYMENT 
def create_deployment():
    github_block = GitHub.load(os.getenv("GITHUB_BLOCK"))
    deployment_elt = Deployment.build_from_flow(
        flow=elt_flow,
        name="Daily Air Quality",
        storage=github_block
    )


# PREFECT BLOCKS
from prefect_gcp import GcpCredentials

GCP_CREDENTIALS_BLOCK_NAME = "gcp-credentials"
GCS_AIR_QUALITY_BUCKET_BLOCK_NAME = "gcs-air-quality-bucket"

def create_gcp_credentials_block(filepath: str = os.getenv("GCP_CREDENTIALS_FILEPATH", "google_creds.json")):
    with open(filepath, "r") as f:
        service_account_info = f.read()

    GcpCredentials(
        service_account_info=service_account_info
    ).save(GCP_CREDENTIALS_BLOCK_NAME)

def create_gcs_bucket_block(bucket_name: str):
    # Creates prefect block that is linked to the GCS Bucket created with terraform
    GcsBucket(
        bucket=bucket_name
    ).save(GCS_AIR_QUALITY_BUCKET_BLOCK_NAME)
    
def create_email_block(email: str):
    # Email for prefect block to send notification to you when pipelines fails
    # To-Do?
    pass
    
def create_github_block():
    # Use this if you are forking the repo and using your own as storage
    GitHub(
        repository=os.environ.get("GITHUB_REPO"),
        reference=os.environ.get("GITHUB_BRANCH"),
        include_git_objects=True
    ).save(os.getenv("GITHUB_BLOCK"))
    
def build_blocks(filepath_gcp_creds: str, aq_bucket_name: str):
    create_gcp_credentials_block(filepath_gcp_creds)
    create_gcs_bucket_block(aq_bucket_name)

if __name__ == "__main__":
    # deployment_elt.apply()
    pass