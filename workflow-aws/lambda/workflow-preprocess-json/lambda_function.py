# Name: workflow-preprocess-json
# Version: 0.1a4

import gzip
from io import StringIO
import json
import os

import boto3
import pandas as pd
import s3fs


def lambda_handler(event, context):

    src = event['src']
    result = {}

    try:
        fs = s3fs.S3FileSystem(anon=False)
        with fs.open(src, 'rb') as f:
            g = gzip.GzipFile(fileobj=f)
            df = pd.read_json(g, lines=True, convert_dates=['date'])
    except BaseException as e:
        print(e)
        raise e
    df = df[['date', 'gross', 'net', 'tax', 'email']]
    buff = StringIO()
    df.to_csv(buff, index=False)
    dst_bucket_name = 'korniichuk.tmp'
    src_filename = os.path.basename(src)
    dst_filename = src_filename.replace('.gz', '.csv')
    dst_key = 'workflow-orders/csv/{}'.format(dst_filename)
    try:
        s3 = boto3.resource('s3')
        s3.Object(dst_bucket_name, dst_key).put(Body=buff.getvalue())
    except BaseException as e:
        print(e)
        raise e
    else:
        dst = os.path.join(dst_bucket_name, dst_key)
        msg = 'Preprocessed data saved to {} file'.format(dst)
        print(msg)
        result['dst'] = dst
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
