from prefect_dbt.cloud.jobs import DbtCloudJob, run_dbt_cloud_job
from prefect_dbt.cloud import DbtCloudCredentials
from prefect import flow


from prefect.flows import healthcheck
from prefect import get_client
from prefect.deployments import Deployment
from prefect.filesystems import GCS
from prefect_gcp.cloud_run import CloudRunJob

client = get_client()

gcs_block = GCS.load("my-gcs-bucket")
cloud_run_job_block = CloudRunJob.load("my-gcp-infrastructure")


deployment = Deployment.build_from_flow(
    flow=healthcheck,
    name="GCP test flow",
    storage=gcs_block,
    infrastructure=cloud_run_job_block,
)

if __name__ == "__main__":
    deployment.apply()

development_run_jobid = 264248

# dbt_cloud_credentials = DbtCloudCredentials.load("dbt-cloud")
# dbt_cloud_job = DbtCloudJob(
#     dbt_cloud_credentials=dbt_cloud_credentials,
#     job_id=development_run_jobid
# ).save("development-run-job")

@flow
def run_dbt_job_flow():
    result = run_dbt_cloud_job(
        dbt_cloud_job=DbtCloudJob.load("development-run-job"),
        targeted_retries=2,
    )
    print(result)

# run_dbt_job_flow()

import pkg_resources
from subprocess import call

def update_pip_packages():
    packages = [dist.project_name for dist in pkg_resources.working_set]

    call("pip install --upgrade " + ' '.join(packages), shell=True)