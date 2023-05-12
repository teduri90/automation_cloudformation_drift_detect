import os
import json
import time
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from datetime import datetime

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

class StackDriftDetector:
    def __init__(self, profile_name, region_name, max_attempts=3):
        self.aws_config = Config(
            region_name=region_name,
            retries={'max_attempts': max_attempts, 'mode': 'standard'}
        )
        session = boto3.session.Session(profile_name=profile_name)
        self.client = session.client(service_name='cloudformation', config=self.aws_config)
        self.stack_objects = {}

    def list_all_stacks(self):
        paginator = self.client.get_paginator('list_stacks')
        for page in paginator.paginate():
            for stack in page['StackSummaries']:
                yield stack

    def fetch_all_stacks(self):
        stacks = []
        for stack in self.list_all_stacks():
            stacks.append(stack['StackName'])
        return stacks

    def detect_stack_drift(self, stack_name):
        try:
            response = self.client.detect_stack_drift(StackName=stack_name)
            self.stack_objects[stack_name] = response['StackDriftDetectionId']
        except ClientError as e:
            if e.response['Error']['Code'] == 'ValidationError' and 'does not exist' in e.response['Error']['Message']:
                print(f"Stack {stack_name} does not exist")
            else:
                raise e

    def check_stack_drift(self, stack_name):
        stack_object_value = self.stack_objects[stack_name]
        response = self.client.describe_stack_drift_detection_status(
            StackDriftDetectionId=stack_object_value
        )

        if response['DetectionStatus'] != "DETECTION_COMPLETE":
            print("Still not done. Sleep for 10 seconds and continue")
            time.sleep(10)
            return self.check_stack_drift(stack_name)

        return {stack_name: [stack_object_value, response['StackDriftStatus']]}

    @staticmethod
    def write_to_file(filename, data):
        with open(filename, 'a') as f:
            f.write(json.dumps(data, cls=DateTimeEncoder))
            f.write("\n")

    def process_stacks(self, filter_text=None, stack_file=None):
        if stack_file and os.path.exists(stack_file):
            with open(stack_file, 'r') as f:
                stacks = [line.strip() for line in f if line.strip()]  # ignore blank lines and lines with white spaces
        else:
            stacks = self.fetch_all_stacks()
            if filter_text:
                stacks = [stack for stack in stacks if filter_text in stack]

        for stack in stacks:
            print(stack)
            self.detect_stack_drift(stack)

        self.write_to_file('temp.txt', self.stack_objects)

        for stack_name in self.stack_objects:
            print(f'{stack_name} stack object')
            result = self.check_stack_drift(stack_name)
            self.write_to_file('result.txt', result)

        print("Complete")

if __name__ == "__main__":
    try:
        detector = StackDriftDetector(profile_name='default', region_name='ap-southeast-1')
        detector.process_stacks(filter_text='Datadog', stack_file="test.txt")
    except ClientError as e:
        raise e

