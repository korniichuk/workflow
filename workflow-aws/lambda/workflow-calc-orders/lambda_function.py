# Name: workflow-calc-orders
# Version: 0.1a1

from io import StringIO
import json
import os

import arrow
import boto3
import pandas as pd


def lambda_handler(event, context):

    result = {}

    src = event['src']
    tmp = pd.read_csv(src, parse_dates=['date'])
    tmp = tmp.groupby('email').count().sort_values('email')
    df = pd.DataFrame()
    df['email'] = tmp.index
    df['orders'] = tmp.date.values
    buff = StringIO()
    df.to_csv(buff, index=False)
    dst_bucket_name = 'korniichuk.demo'
    date = arrow.utcnow().format('YYYYMMDD')
    dst_filename = 'orders_{}.csv'.format(date)
    dst_key = 'workflow/output/{}'.format(dst_filename)
    try:
        s3 = boto3.resource('s3')
        s3.Object(dst_bucket_name, dst_key).put(Body=buff.getvalue())
    except BaseException as e:
        print(e)
        raise e
    else:
        dst = 's3://' + os.path.join(dst_bucket_name, dst_key)
        msg = 'Orders saved to {} file'.format(dst)
        print(msg)
        result['dst'] = dst
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
