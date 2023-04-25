# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug  9 10:26:06 2022

@author: imane.hafnaoui
"""

import logging

import boto3, os
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
sqs = boto3.resource('sqs', region_name=os.environ['AWS_REGION'])

# SQS_MAX_MSGS_RECEIVED = int(os.environ['SQS_MAX_MSGS_RECEIVED'])
# SQS_VIS_TIMEOUT = int(os.environ['SQS_VIS_TIMEOUT'])
# SQS_POLLING_TIME = int(os.environ['SQS_POLLING_TIME'])

def get_queue(name):
    """
    Gets an SQS queue by name.

    :param name: The name that was used to create the queue.
    :return: A Queue object.
    """
    try:
        queue = sqs.get_queue_by_name(QueueName=name)
        logger.info("Got queue '%s' with URL=%s", name, queue.url)
    except ClientError as error:
        logger.exception("Couldn't get queue named %s.", name)
        raise error
    else:
        return queue


def get_queue_messages(queue):
    queue_messages = queue.receive_messages()
        # MaxNumberOfMessages=SQS_MAX_MSGS_RECEIVED, 
                                            # VisibilityTimeout=SQS_VIS_TIMEOUT,
                                            # WaitTimeSeconds=SQS_POLLING_TIME)
    logger.info(f"Loaded [{len(queue_messages)}] messages in the queue.")
    
    return queue_messages