# BEIS BRE ORP Data Service

This repository contains AWS Lambda functions (found in the `lambdas/` directory) that process legislative documents uploaded to the ORP. The ORP currently supports documents in the form of a PDF, DOCX, ODF and URLs pointing the HTML documents. When a document is uploaded to the ORP and placed in an S3 bucket, the ORP ingestion pipeline makes use of these lambda functions. The functions extract metadata, raw text, legislative origin, and use data science techniques to infer appropriate titles, extract keywords, extract a summary, and check if the document has already been uploaded to the ORP. Once these functions have finished, a final function runs to insert the metadata and extracted data into a graph database. Once ingested, the document is available for search and download.

This software is designed to be used by organizations that need to process UK legislative material.

## Architecture

The AWS Lambda functions in this repository are orchestrated by Step Functions, which is defined in Terraform in a separate repository - see [beis-orp-infrastructure](https://github.com/mdrxtech/beis-orp-infrastructure). The Terraform configuration creates the necessary resources in AWS, including the Step Functions state machine, IAM roles and policies, and CloudWatch Logs groups. It also creates all supporting infrastructure and other infrastructure including the front end application, graph database and search functionality. All this infrastructure makes up the entirety of the ORP.

## Prerequisites

- AWS account with appropriate permissions to create and deploy Lambda functions, Step Functions, and other necessary resources
- Docker installed and running
- AWS CLI installed
- Terraform installed

## Monitoring

The health of the pipeline can be monitored using the `pipeline-monitoring` CloudWatch dashboard and the alarms configured in CloudWatch. For more information, please refer to the monitoring runbook.

## Maintenance/Deployment

To update the Lambda functions or Docker image, follow the steps in the Maintenance/Deployment Runbook provided in this repository. It is recommended that you test any changes to the software in a non-production environment before deploying them to a production environment.

To update the Step Functions state machine or other architecture resources, use the Terraform configuration in the infrastructure repository.

## Troubleshooting

If you encounter any issues with the Lambda functions, Docker image, Step Functions, Terraform configuration, or other AWS deployment resources, refer to the Troubleshooting Runbook provided in this repository or contact the development team for assistance.

## Runbooks

- [Monitoring Runbook](./runbooks/monitoring.md)
- [Maintenance/Deployment Runbook](./runbooks/maintenance_deployment.md)
- [Troubleshooting Runbook](./runbooks/troubleshooting.md)

## License

This repository is licensed under the MIT License.
