# BEIS BRE ORP Data Service

This is the data service repository for the BEIS infrastructure being deployed to AWS. The repository contains all of the modular logic used as part of the ingestion pipeline for the BEIS BRE ORP. AWS Lambda functions are defined in the `lambdas/` directory. There are deployed as docker containers and are combined in AWS Step Functions to form the entire ingestion pipeline.

For the Step Functions state machine definition and the rest of the Terraform deployment, see the [beis-orp-infrastructure](https://github.com/mdrxtech/beis-orp-infrastructure) repository.

### To Upload a Docker Image to Amazon ECR Repo
`sh utils/aws_image_uploader.sh <ACCOUNT_ID> <IMAGE_NAME> <TAG>`