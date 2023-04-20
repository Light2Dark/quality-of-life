from elt_web_to_bq import elt_flow, test_elt_flow
from prefect import get_client
from prefect.deployments import Deployment
from prefect_gcp.cloud_run import CloudRunJob    
from prefect.filesystems import GitHub

client = get_client()

github_block = GitHub.load("quality-of-life")
cloud_run_job_block = CloudRunJob.load("daily-air-quality")


deployment_cloud_run = Deployment.build_from_flow(
    flow=test_elt_flow,
    name="GCP test flow",
    storage=github_block,
    infrastructure=cloud_run_job_block
)

deployment_elt = Deployment.build_from_flow(
    flow=elt_flow,
    name="Daily Air Quality",
    storage=github_block   
)

if __name__ == "__main__":
    deployment_elt.apply()