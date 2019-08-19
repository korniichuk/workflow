# Name: workflow-merge-csvs
# Version: 0.1a5

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
    dst_filename = 'transactions_{}.csv'.format(date)
    dst_key = 'workflow/output/{}'.format(dst_filename)
    dst = 's3://' + os.path.join(dst_bucket, dst_key)
    result['path'] = dst
    if exists_in_s3(dst_bucket, dst_key):
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
    keys = []
    s3 = boto3.resource('s3')
    src_bucket = 'korniichuk.tmp'
    for obj in s3.Bucket(src_bucket).objects.all():
        key = obj.key
        if key.startswith('workflow-orders/csv/') and key.endswith('.csv'):
            keys.append(key)
    csvs = [BytesIO(s3.Object(src_bucket, key).get()['Body'].read())
            for key in keys]
    df = pd.concat([pd.read_csv(csv, parse_dates=['date']) for csv in csvs])
    buff = StringIO()
    df.to_csv(buff, index=False)
    s3.Object(dst_bucket, dst_key).put(Body=buff.getvalue())
    msg = 'Merged data saved to {} file'.format(dst)
    print(msg)
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
