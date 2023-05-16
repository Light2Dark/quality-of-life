from pipelines.air_quality import test_elt_flow, elt_flow
from prefect import get_client
from prefect.deployments import Deployment 
from prefect.filesystems import GitHub

client = get_client()

github_block = GitHub.load("quality-of-life")

# PREFECT DEPLOYMENT 

deployment_elt = Deployment.build_from_flow(
    flow=elt_flow,
    name="Daily Air Quality",
    storage=github_block   
)


# PREFECT BLOCKS

from prefect_gcp import GcpCredentials

service_account_info = {
  "type": "service_account",
  "project_id": "PROJECT_ID",
  "private_key_id": "KEY_ID",
  "private_key": "-----BEGIN PRIVATE KEY-----\nPRIVATE_KEY\n-----END PRIVATE KEY-----\n",
  "client_email": "SERVICE_ACCOUNT_EMAIL",
  "client_id": "CLIENT_ID",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://accounts.google.com/o/oauth2/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/SERVICE_ACCOUNT_EMAIL"
}

GcpCredentials(
    service_account_info=service_account_info
).save("BLOCK-NAME-PLACEHOLDER")


if __name__ == "__main__":
    # deployment_elt.apply()
    pass