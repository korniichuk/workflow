# Name: workflow-preprocess-jsons
# Version: 0.1a2

import json
import os

import boto3


def lambda_handler(event, context):

    bucket_name = 'korniichuk.demo'
    lmbd_name = 'workflow-preprocess-json'
    result = {'keys': []}

    s3 = boto3.resource('s3')
    lmbd = boto3.client('lambda')
    for obj in s3.Bucket(bucket_name).objects.all():
        key = obj.key
        if key.startswith('workflow/input/') and key.endswith('.gz'):
            result['keys'].append(key)
            src = os.path.join(bucket_name, key)
            data = {'src': src}
            payload = json.dumps(data)
            lmbd.invoke(FunctionName=lmbd_name,
                        InvocationType='Event',
                        Payload=payload)
    result['num'] = len(result['keys'])
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
