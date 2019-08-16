# Name: workflow-calc-orders
# Version: 0.1a2

from io import StringIO
import json
import os

import arrow
import boto3
import botocore
import pandas as pd


def exists_in_s3(bucket_name, key):
    """Does object exist in S3 bucket"""

    s3 = boto3.resource('s3')
    try:
        s3.Object(bucket_name, key).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            msg = 'Error: {} object checking in {} S3 bucket is failed'.format(
                    key, bucket_name)
            print(msg)
            return False
    else:
        return True


def lambda_handler(event, context):
    result = {}
    dst_bucket_name = 'korniichuk.demo'
    date = arrow.utcnow().format('YYYYMMDD')
    dst_filename = 'orders_{}.csv'.format(date)
    dst_key = 'workflow/output/{}'.format(dst_filename)
    dst = 's3://' + os.path.join(dst_bucket_name, dst_key)
    result['dst'] = dst
    if exists_in_s3(dst_bucket_name, dst_key):
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
    src = event['src']
    tmp = pd.read_csv(src, parse_dates=['date'])
    tmp = tmp.groupby('email').count().sort_values('email')
    df = pd.DataFrame()
    df['email'] = tmp.index
    df['orders'] = tmp.date.values
    buff = StringIO()
    df.to_csv(buff, index=False)
    s3 = boto3.resource('s3')
    s3.Object(dst_bucket_name, dst_key).put(Body=buff.getvalue())
    msg = 'Orders saved to {} file'.format(dst)
    print(msg)
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
