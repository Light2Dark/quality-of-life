#!usr/bin/bash

# This script is used to setup the final infrastructure for the project.
cd infra

terraform init
terraform plan
terraform apply

cd ..

# Use terraform output to obtain variables to create prefect blocks
python -c "import prefect_infra; prefect_infra.build_blocks(filepath_gcp_creds=${terraform output gcp_credentials}, aq_bucket_name=${terraform output gcs_aq_bucket_name})"

# python -c "import prefect_infra; prefect_infra.create_github_credentials_block(${terraform output github_credentials})"