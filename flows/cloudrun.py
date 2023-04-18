from prefect_github import GitHubCredentials
github_credentials_block = GitHubCredentials.load("github-credentials")

# from prefect import get_client
# client = get_client()

def run_dbt_job_flow():
    from prefect_dbt.cloud.jobs import DbtCloudJob, run_dbt_cloud_job
    from prefect_dbt.cloud import DbtCloudCredentials
    
    development_run_jobid = 264248
    
    dbt_cloud_credentials = DbtCloudCredentials.load("dbt-cloud")
    dbt_cloud_job = DbtCloudJob(
        dbt_cloud_credentials=dbt_cloud_credentials,
        job_id=development_run_jobid
    ).save("development-run-job")
    
    result = run_dbt_cloud_job(
        dbt_cloud_job=DbtCloudJob.load("development-run-job"),
        targeted_retries=2,
    )
    print(result)


def update_pip_packages():
    import pkg_resources
    from subprocess import call
    packages = [dist.project_name for dist in pkg_resources.working_set]
    call("pip install --upgrade " + ' '.join(packages), shell=True)
    
    
    
if __name__ == "__main__":
    # deployment.apply()
    pass