# Monitoring Runbook
## Overview

This runbook provides instructions for monitoring the health and performance of the AWS Step Functions and Lambda Functions used in the ORP ingestion pipeline.

## Prerequisites

- AWS account with appropriate permissions to access CloudWatch metrics and alarms.
- Access to the `pipeline-monitoring` dashboard in CloudWatch.
- Access to the Terraform repository defining the dashboard, metrics and alarms.

## Monitoring

To monitor the health and performance of the Step Functions and Lambda Functions, follow these steps:
- Log in to the AWS Management Console.
- Navigate to the CloudWatch service. 
- In the left-hand menu, click on Dashboards. 
- Locate the pipeline-monitoring dashboard and click on it.

The dashboard will display metrics for the following (both at the pipeline level and individual function level):
- Executions: This metric displays the number of executions for Step Functions and the Lambda Functions. If this metric is higher than usual, it may indicate increased usage or an issue with the pipeline.
- Duration: This metric displays the duration of each execution for Step Functions and the Lambda Functions. If this metric is higher than usual, it may indicate a performance issue that requires investigation.
- Success Percentage: This metric displays the success percentage for Step Functions and the Lambda Functions. If this metric is lower than usual, it may indicate an issue with the pipeline or a bug in the software.

## Alerts

The following alerts are currently defined and notify OpenRegulationPlatform@beis.gov.uk when they are in a triggered state:

- Pipeline Abnormal Throughput: This alarm triggers when the pipeline is processing a number of documents that is more than 2 standard deviations outside the expected number.
- Step Function Large Execution Time: This alarm triggers when the duration of each execution of the pipeline is more than 2 standard deviations higher than the expected duration.
- Step Function Low Success Rate: This alarm triggers when the success rate of the pipeline drops below 90% on any day. 
- Step Function Throttling: This alarm triggers when there are throttled executions in the pipeline. When this occurs, it may indicate a capacity issue that requires attention.
- Lambda Large Duration: This alarm triggers when the duration of a lambda function is more than 10 minutes. 
- Lambda Low Success Rate: This alarm triggers when the success rate of the lambda functions drops below 90% on any day. 
- Lambda Throttling: This alarm triggers when the lambda functions are being throttled. When this occurs, it may indicate a capacity issue that requires attention.

If any of the alarms go off, investigate the issue by checking the logs in CloudWatch Logs and the Step Functions and Lambda Functions themselves.