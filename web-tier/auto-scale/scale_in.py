#!/usr/bin/python3

import os, sys
import boto3
from botocore.config import Config
from dotenv import load_dotenv
import time
import json

load_dotenv()

# Min number of instances that wait for an additional 1.5 minutes before terminating
# Here, we will check if there are 0 messages in queue for another 1.5 minutes before terminating the last instance
MIN_INSTANCES_WAIT_OUT = 1
SLEEP_TIME = 30

# AWS Config
REGION = os.getenv("REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
REQUEST_QUEUE_URL = os.getenv("REQUEST_QUEUE_URL")
AMI = os.getenv("AMI")
CONFIG = Config(region_name=REGION)

SQS_CLIENT = boto3.client('sqs',
            config=CONFIG,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
EC2_CLIENT = boto3.client('ec2',
            config=CONFIG,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
SESSION = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
EC2_RESOURCE = boto3.resource('ec2', 
            config=CONFIG,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )


# Get the count of the messages in the request queue. (SQS - ApproximateNumberOfMessages)
def get_count_request_queue():
    count_request_queue = SQS_CLIENT.get_queue_attributes(QueueUrl=REQUEST_QUEUE_URL,
                                                          AttributeNames=['ApproximateNumberOfMessages']
                                                        )
    if count_request_queue is None or \
            'Attributes' not in count_request_queue.keys() or \
            'ApproximateNumberOfMessages' not in count_request_queue['Attributes'].keys():
        return 0
    return count_request_queue['Attributes']['ApproximateNumberOfMessages']


# Get the instance_id of all the instances based on the filters: (1) AMI ID and (2) Running Status
def get_ec2_instances(status):
    filters = [
        {
            'Name': 'image-id',
            'Values': [AMI]
        },
        {
            'Name': 'instance-state-name',
            'Values': [status]
        },
    ]
    instances = EC2_RESOURCE.instances.filter(Filters=filters)
    return [instance.id for instance in instances]


# Responsible for scaling-in EC2 instances. Does so by randomly selection 2 instances to scale in and renames the rest. 
def scale_in():
    # Extract current 'running' instances
    filters = [
        {
            'Name': 'image-id',
            'Values': [AMI]
        },
        {
            'Name': 'instance-state-name',
            'Values': ['running']
        },
    ]
    instances = EC2_RESOURCE.instances.filter(Filters=filters)
    total_instances = len([instance.id for instance in instances] )
    if total_instances == 0:
        return

    # Scale in one instance
    for instance in instances:
        response = EC2_CLIENT.terminate_instances(InstanceIds=[instance.id])
        break
    

if __name__ == "__main__":

    message_counts = []
    print("Scale in service started...")
    sys.stdout.flush()
    count = 0
    # Keep getting the number of messages in the queue every 5 minutes
    while True:
        current_messages = get_count_request_queue()
        if len(message_counts) < 3:
            message_counts.append(int(current_messages))
        else:
            message_counts = message_counts[1:]
            message_counts.append(int(current_messages))
            avg_messages = sum(message_counts) / 3
            
            if avg_messages == 0.0:
                current_instances = len(get_ec2_instances('running'))
                if current_instances == MIN_INSTANCES_WAIT_OUT:
                    count += 1
                    if count == 3:
                        scale_in()
                        count = 0
                else:
                    count = 0
                    scale_in()
            else:
                count = 0

        time.sleep(SLEEP_TIME)
