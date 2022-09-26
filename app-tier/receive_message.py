#!/usr/bin/python3

import torch
import torchvision
import torchvision.transforms as transforms
import torch.nn as nn
import torch.nn.functional as F
import torchvision.models as models
from urllib.request import urlopen
from PIL import Image
import numpy as np

import os, sys
import boto3
from botocore.config import Config
from dotenv import load_dotenv
import time
import json

load_dotenv()

REGION=os.getenv("REGION")
AWS_ACCESS_KEY_ID=os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY=os.getenv("AWS_SECRET_ACCESS_KEY")
REQUEST_QUEUE_URL=os.getenv("REQUEST_QUEUE_URL")
RESPONSE_QUEUE_URL=os.getenv("RESPONSE_QUEUE_URL")
S3_BUCKET_INPUT_IMAGES=os.getenv("S3_BUCKET_INPUT_IMAGES")
S3_BUCKET_RESULTS=os.getenv("S3_BUCKET_RESULTS")

CONFIG = Config(region_name=REGION)

SQS_CLIENT = boto3.client('sqs',
            config=CONFIG,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
SESSION = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
S3_RESOURCE = SESSION.resource('s3')
S3_INPUT_IMAGES_BUCKET = S3_RESOURCE.Bucket(S3_BUCKET_INPUT_IMAGES)
S3_RESULTS_BUCKET = S3_RESOURCE.Bucket(S3_BUCKET_RESULTS)


def classify_image(image_body, s3_key) -> (str, str):
    image = Image.open(image_body)

    model = models.resnet18(pretrained=True)
    model.eval()
    img_tensor = transforms.ToTensor()(image).unsqueeze_(0)
    outputs = model(img_tensor)
    _, predicted = torch.max(outputs.data, 1)

    with open('/home/ubuntu/classifier/imagenet-labels.json') as f:
        labels = json.load(f)
    result = labels[np.array(predicted)[0]]
    image_name = s3_key.split("/")[-1][:-5]

    return image_name, result


def get_s3_image(image_data):
    s3_image_body = ""
    s3_key = image_data['s3_key']
    try:
        s3_image = S3_INPUT_IMAGES_BUCKET.Object(s3_key).get()
        s3_image_body = s3_image['Body']
    except Exception as e:
        pass

    return s3_image_body, s3_key


def process_result(image_name, image_data, result, receipt_handle, request_time, response_time):
    try:
        value = (image_name, result)
        
        image_data['result'] = result
        timestamps = {'request': request_time, 'response': response_time}
        if 'timestamps' in image_data.keys():
            image_data['timestamps']['apptier'] = timestamps
        else:
            image_data['timestamps'] = {'apptier': timestamps}

        S3_RESULTS_BUCKET.put_object(Key=image_name, Body=bytearray(str(value), "utf-8"))
        SQS_CLIENT.send_message(QueueUrl=RESPONSE_QUEUE_URL, MessageBody=json.dumps(image_data))
    except Exception as e:
        return
    
    # We need to delete the successfully processed message so that it is not picked up by another EC2 instance again
    SQS_CLIENT.delete_message(QueueUrl=REQUEST_QUEUE_URL, ReceiptHandle=receipt_handle)


if __name__ == "__main__":

    while True:

        response = ""
        try:
            response = SQS_CLIENT.receive_message(QueueUrl=REQUEST_QUEUE_URL)
        except Exception as e:
            pass
            
        if len(response) > 0 and 'Messages' in response.keys():

            for message in response['Messages']:

                request_time = int(time.time() * 1000)

                # Get and parse message body
                body = message['Body']
                receipt_handle = message['ReceiptHandle']
                
                image_data = ""
                try:
                    image_data = json.loads(body)
                except Exception as e:
                    pass
                
                print(f"Image data: {image_data}")
                sys.stdout.flush()

                if len(image_data) == 0:
                    continue
                
                # Get image from S3
                s3_image_body, s3_key = get_s3_image(image_data)
                if s3_image_body is None:
                    continue

                # Claassify the image
                image_name, result = classify_image(s3_image_body, s3_key)

                # Don't do anything beyond this point if response time - request time > 30 seconds
                response_time = int(time.time() * 1000)
                if((response_time - request_time) / 1000 > 30):
                    continue
                
                if len(result) > 0:
                    process_result(image_name, image_data, result, receipt_handle, request_time, response_time)
                else:
                    continue

        time.sleep(0.1)
