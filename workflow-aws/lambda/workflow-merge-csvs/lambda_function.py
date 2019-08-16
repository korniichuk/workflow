# Name: workflow-merge-csvs
# Version: 0.1a1

from io import StringIO
import json
import os

import arrow
import boto3
import pandas as pd
import s3fs


def lambda_handler(event, context):
    result = {}
    fs = s3fs.S3FileSystem(anon=False)
    csvs = fs.glob('korniichuk.tmp/workflow-orders/csv/*.csv')
    csvs = ['s3://' + csv for csv in csvs]
    df = pd.concat([pd.read_csv(csv) for csv in csvs])
    buff = StringIO()
    df.to_csv(buff, index=False)
    dst_bucket_name = 'korniichuk.demo'
    date = arrow.utcnow().format('YYYYMMDD')
    dst_filename = 'transactions_{}.csv'.format(date)
    dst_key = 'workflow/output/{}'.format(dst_filename)
    try:
        s3 = boto3.resource('s3')
        s3.Object(dst_bucket_name, dst_key).put(Body=buff.getvalue())
    except BaseException as e:
        print(e)
        raise e
    else:
        dst = 's3://' + os.path.join(dst_bucket_name, dst_key)
        msg = 'Merged data saved to {} file'.format(dst)
        print(msg)
        result['dst'] = dst
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
