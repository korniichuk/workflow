# Name: workflow-calc-orders
# Version: 0.1a4

from io import BytesIO, StringIO
import json
import os

import arrow
import boto3
import botocore
import pandas as pd


def exists_in_s3(bucket, key):
    """Does object exist in S3 bucket"""

    s3 = boto3.resource('s3')
    try:
        s3.Object(bucket, key).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            msg = 'Error: {} object checking in {} S3 bucket is failed'.format(
                    key, bucket)
            print(msg)
            return False
    else:
        return True


def lambda_handler(event, context):
    result = {}
    dst_bucket = 'korniichuk.demo'
    date = arrow.utcnow().format('YYYYMMDD')
    dst_filename = 'orders_{}.csv'.format(date)
    dst_key = 'workflow/output/{}'.format(dst_filename)
    dst = 's3://' + os.path.join(dst_bucket, dst_key)
    result['path'] = dst
    if exists_in_s3(dst_bucket, dst_key):
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
    body = json.loads(event['body'])
    src = body['path']
    src_bucket = src.replace('s3://', '').split('/')[0]
    src_key = src.replace('s3://', '')[len(src_bucket)+1:]
    s3 = boto3.resource('s3')
    f = BytesIO(s3.Object(src_bucket, src_key).get()['Body'].read())
    tmp = pd.read_csv(f, parse_dates=['date'])
    tmp = tmp.groupby('email').count().sort_values('email')
    df = pd.DataFrame()
    df['email'] = tmp.index
    df['orders'] = tmp.date.values
    buff = StringIO()
    df.to_csv(buff, index=False)
    s3.Object(dst_bucket, dst_key).put(Body=buff.getvalue())
    msg = 'Orders saved to {} file'.format(dst)
    print(msg)
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
