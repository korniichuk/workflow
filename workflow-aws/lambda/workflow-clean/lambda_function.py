# Name: workflow-clean
# Version: 0.1a1

import json
import os

import boto3


def lambda_handler(event, context):
    bucket = 'korniichuk.tmp'
    result = {'paths': []}
    s3 = boto3.resource('s3')
    for obj in s3.Bucket(bucket).objects.all():
        key = obj.key
        if key.startswith('workflow-orders'):
            src = 's3://' + os.path.join(bucket, key)
            result['paths'].append(src)
            obj.delete()
    result['num'] = len(result['paths'])
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
