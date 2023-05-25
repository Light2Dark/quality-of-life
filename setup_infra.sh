#!usr/bin/bash
# This script is used to setup the final infrastructure for the project.

cd infra

terraform init
terraform plan
terraform apply

# Activate source variables
source ../.env

# Create prefect profile
PREFECT_PROFILE_NAME="new_profile"
prefect profile create ${PREFECT_PROFILE_NAME}
prefect profile use ${PREFECT_PROFILE_NAME}
prefect config set PREFECT_API_KEY=${PREFECT_API_KEY} PREFECT_API_URL="https://api.prefect.cloud/api/accounts/${PREFECT_API_ACCOUNT_ID}/workspaces/${PREFECT_API_WORKSPACE_ID}"

# Authenticate to prefect cloud
prefect cloud login -k ${PREFECT_API_KEY}

# Use terraform output to obtain variables to create prefect blocks
python -c "import prefect_infra; prefect_infra.build_blocks(filepath_gcp_creds=$(terraform output gcp_credentials), aq_bucket_name=$(terraform output gcs_aq_bucket_name), weather_bucket_name=$(terraform output gcs_weather_bucket_name)"

# Back to root directory
cd ..

# python -c "import prefect_infra; prefect_infra.create_github_credentials_block(${terraform output github_credentials})"