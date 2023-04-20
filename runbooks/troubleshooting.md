# Troubleshooting Runbook
## Prerequisites

- AWS CLI installed and configured
- Docker installed and running
- Terraform installed

## Steps

1. Check the AWS CloudWatch logs for the Lambda functions to identify any errors or warning messages.
2. Use the AWS Lambda console to test the Lambda functions with sample input data and verify that the expected output is produced.
3. Check the Docker logs for any issues related to the container or the Docker image.
4. Check the AWS S3 bucket for any issues related to the uploaded documents or data.
5. If necessary, update the Lambda functions or Docker image (see Maintenance/Deployment Runbook for steps).
6. If the issue persists, contact the development team for further assistance.