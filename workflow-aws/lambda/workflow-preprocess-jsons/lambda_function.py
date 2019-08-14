# Name: workflow-preprocess-jsons
# Version: 0.1a1

import json

import boto3


def lambda_handler(event, context):

    bucket_name = 'korniichuk.demo'
    lmbd_name = 'workflow-decompress-single'
    result = {'keys': []}

    s3 = boto3.resource('s3')
    lmbd = boto3.client('lambda')
    for obj in s3.Bucket(bucket_name).objects.all():
        key = obj.key
        if key.startswith('workflow/input/') and key.endswith('.gz'):
            result['keys'].append(key)
            r = lmbd.invoke(FunctionName=lmbd_name, InvocationType='Event')
    result['num'] = len(result['keys'])
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
