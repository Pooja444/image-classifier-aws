#!/usr/bin/python3

from collections import deque
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

import time
import boto3
import logging
import os, sys

load_dotenv()

# Constants
REGION = os.getenv("REGION")
INSTANCE_TYPE = os.getenv("INSTANCE_TYPE")
AMI = os.getenv("AMI")
ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
SECURITY_GROUP_ID = os.getenv("SECURITY_GROUP_ID")
SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
REQUEST_QUEUE_URL = os.getenv("REQUEST_QUEUE_URL")
SLEEP_TIME = 30
SCALE_OUT_LIMIT = 20
CONFIG = Config(region_name=REGION)
SQS_CLIENT = boto3.client('sqs',
                          config=CONFIG,
                          aws_access_key_id=ACCESS_KEY_ID,
                          aws_secret_access_key=SECRET_ACCESS_KEY)
EC2_RESOURCE = boto3.resource('ec2',
                              config=CONFIG,
                              aws_access_key_id=ACCESS_KEY_ID,
                              aws_secret_access_key=SECRET_ACCESS_KEY)
logging.basicConfig(filename='/home/ubuntu/logs/scale_out-log.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)
USER_DATA="""#!/bin/bash
runuser -l ubuntu -c 'cd /home/ubuntu/classifier ; nohup python3 receive_message.py &'"""


# Get the count of the messages in the request queue. (SQS - ApproximateNumberOfMessages)
def get_count_request_queue():
    count_request_queue = SQS_CLIENT.get_queue_attributes(QueueUrl=REQUEST_QUEUE_URL,
                                                          AttributeNames=['ApproximateNumberOfMessages'])
    if count_request_queue is None or \
            'Attributes' not in count_request_queue or \
            'ApproximateNumberOfMessages' not in count_request_queue['Attributes']:
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


# Create EC2 instances when provided min_count and max_count.
def create_ec2_instance(min_count=1, max_count=1):
    try:
        # Create EC2 instances.
        instances = EC2_RESOURCE.create_instances(ImageId=AMI,
                                                  MinCount=min_count,
                                                  MaxCount=max_count,
                                                  InstanceType=INSTANCE_TYPE,
                                                  SecurityGroupIds=[SECURITY_GROUP_ID],
                                                  UserData=USER_DATA)

        # Get all instances.
        all_instances = []
        ec2_instances_running = get_ec2_instances('running')
        ec2_instances_pending = get_ec2_instances('pending')
        all_instances.extend(ec2_instances_running)
        all_instances.extend(ec2_instances_pending)
        # Get number of running instances
        current_instance_count = len(all_instances)

        # Rename all the instances that are freshly launched.
        for instance in instances:
            current_instance_count+=1
            EC2_RESOURCE.create_tags(Resources=[instance.id], Tags=[
                {
                    'Key': 'Name',
                    'Value': "app-instance-" + str(current_instance_count),
                },
            ])
        logging.info("Number of instances launched: " + str(len(instances)))
    except ClientError as e:
        print("Unexpected error: %s" % e)


def scale_out():
    count_request_queue = int(get_count_request_queue())
    ec2_instances_running = get_ec2_instances('running')
    ec2_instances_pending = get_ec2_instances('pending')

    # If instances are in pending state, check after timeout if we need more, otherwise the pending instances are probably enough
    if len(ec2_instances_pending) > 0:
        return

    total_instances = len(ec2_instances_running) + len(ec2_instances_pending)
    if (count_request_queue == 0 or total_instances >= 20):
        return

    to_launch = min(20-total_instances, count_request_queue)
    
    global enable_sleep
    enable_sleep = True
    
    logging.debug("Number of messages in queue: " + str(count_request_queue))
    create_ec2_instance(min_count=to_launch, max_count=to_launch)


print("Scale out service started...")
sys.stdout.flush()
enable_sleep = False
while True:
    scale_out()
    while (enable_sleep):
        time.sleep(SLEEP_TIME)
