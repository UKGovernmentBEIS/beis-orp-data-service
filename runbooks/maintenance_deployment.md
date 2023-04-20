# Maintenance/Deployment

If you need to make changes to the pipeline, you can do so by modifying the code in the Lambda functions and then deploy it using the following processes. To change the pipeline structure in Step Functions make changes in the infrastructure repository and then redeploy the pipeline using Terraform.

## Base Checks

Every few months, the Docker base image and the Python packages should be checked to ensure they are still relevant and do not contain any security flaws. If needed, they should be updated accordingly and redeployed.

## CI/CD

This repository contains a GitHub Actions workflow that automatically detects changes to the lambda functions, builds and pushes the docker image and updates the lambda function accordingly. Due to project time constraints there are some caveats to be aware of:
- The docker repository for the lambda must already exist in the AWS account or the workflow will fail
- Only changes to one lambda can be pushed at a time or the workflow will fail
- The workflow must have access to secrets that allow it permissions to push and deploy in AWS

## Manual
To manually deploy the lambda functions follow these steps:

1. Clone the repository to your local machine
2. Navigate to the `lambdas/<function>` directory
3. Build the Docker image using the following command:

>> `docker buildx build --platform linux/amd64 -t <image-name>:<tag> .`

4. Push the Docker image to Amazon ECR using the following commands:

>> `aws ecr get-login-password --region <aws-region> | docker login --username AWS --password-stdin <aws-account-id>.dkr.ecr.<aws-region>.amazonaws.com`

>> `aws ecr create-repository --repository-name <image-name> --image-scanning-configuration scanOnPush=true --image-tag-mutability MUTABLE`

>> `docker tag <image-name>:latest <aws-account-id>.dkr.ecr.<aws-region>.amazonaws.com/<image-name>:<tag>`

>> `docker push <aws-account-id>.dkr.ecr.<aws-region>.amazonaws.com/<image-name>:<tag>`