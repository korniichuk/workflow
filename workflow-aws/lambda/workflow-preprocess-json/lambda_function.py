# Name: workflow-preprocess-json
# Version: 0.1a5

import gzip
from io import StringIO
import json
import os

import boto3
import botocore
import pandas as pd
import s3fs


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
    src = event['src']
    src_filename = os.path.basename(src)
    dst_filename = src_filename.replace('.gz', '.csv')
    dst_bucket_name = 'korniichuk.tmp'
    dst_key = 'workflow-orders/csv/{}'.format(dst_filename)
    dst = 's3://' + os.path.join(dst_bucket_name, dst_key)
    result['dst'] = dst
    if exists_in_s3(dst_bucket_name, dst_key):
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
    fs = s3fs.S3FileSystem(anon=False, default_fill_cache=False)
    with fs.open(src, 'rb') as f:
        g = gzip.GzipFile(fileobj=f)
        df = pd.read_json(g, lines=True, convert_dates=['date'])
    df = df[['date', 'gross', 'net', 'tax', 'email']]
    buff = StringIO()
    df.to_csv(buff, index=False)
    s3 = boto3.resource('s3')
    s3.Object(dst_bucket_name, dst_key).put(Body=buff.getvalue())
    msg = 'Preprocessed data saved to {} file'.format(dst)
    print(msg)
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
