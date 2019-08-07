#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Version: 0.1a12

import glob
import datetime
import os
from subprocess import check_call

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import arrow
import boto3
import botocore
import pandas as pd


def exists_in_s3(bucket_name, path):
    """Does object exist in S3 bucket"""

    s3 = boto3.resource('s3')
    try:
        s3.Object(bucket_name, path).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            msg = 'Error: {} object checking in {} S3 bucket is failed'.format(
                    path, bucket_name)
            print(msg)
            return False
    else:
        return True


def decompress():
    src = '/tmp/airflow-orders/gz'
    dst = '/tmp/airflow-orders/json'
    if os.path.exists(dst) and os.path.isdir(dst):
        return dst
    os.makedirs(dst)
    cwd = os.getcwd()
    os.chdir(src)
    for root, dirs, files in os.walk(src, topdown=False):
        for name in files:
            if name.endswith('.gz'):
                file_abs_path = os.path.join(dst, name.replace('.gz', ''))
                command = 'zcat {} > {}'.format(name, file_abs_path)
                try:
                    check_call(command, shell=True)
                except BaseException:
                    msg = 'Error: {} file decompression is failed'.format(name)
                    print(msg)
                else:
                    msg = '{} file decompressed'.format(name)
                    print(msg)
    # Reset cwd
    os.chdir(cwd)
    return dst


def preprocess_jsons(**context):
    src = context['ti'].xcom_pull(task_ids='decompress')
    dst = '/tmp/airflow-orders/csv'
    if os.path.exists(dst) and os.path.isdir(dst):
        return dst
    os.makedirs(dst)
    cwd = os.getcwd()
    os.chdir(src)
    for root, dirs, files in os.walk(src, topdown=False):
        for name in files:
            ext = os.path.splitext(name)[1]
            if ext == '':
                df = pd.read_json(name, lines=True, convert_dates=['date'])
                df = df[['date', 'gross', 'net', 'tax', 'email']]
                filename = name + '.csv'
                file_abs_path = os.path.join(dst, filename)
                df.to_csv(file_abs_path, index=False)
                msg = 'Preprocessed data saved to {} file'.format(
                        file_abs_path)
                print(msg)
    # Reset cwd
    os.chdir(cwd)
    return dst


def merge_csvs(**context):
    src = context['ti'].xcom_pull(task_ids='preprocess_jsons')
    date = arrow.utcnow().format('YYYYMMDD')
    dst_filename = 'transactions_{}.csv'.format(date)
    dst = os.path.join(src, dst_filename)
    if os.path.exists(dst) and os.path.isfile(dst):
        return dst
    cwd = os.getcwd()
    os.chdir(src)
    csvs = [i for i in glob.glob('*.{}'.format('csv'))]
    df = pd.concat([pd.read_csv(csv) for csv in csvs])
    df.to_csv(dst, index=False)
    # Reset cwd
    os.chdir(cwd)
    return dst


def upload_transactions_to_s3(**context):
    bucket_name = 'korniichuk.demo'
    src = context['ti'].xcom_pull(task_ids='merge_csvs')
    dst_dirname = 'workflow/output'
    dst_filename = os.path.basename(src)
    dst = os.path.join(dst_dirname, dst_filename)
    if not exists_in_s3(bucket_name, dst):
        s3 = boto3.resource('s3')
        s3.Bucket(bucket_name).upload_file(src, dst)
    return dst


def calc_orders(**context):
    src = context['ti'].xcom_pull(task_ids='merge_csvs')
    dts_dirname = os.path.dirname(src)
    date = arrow.utcnow().format('YYYYMMDD')
    dst_filename = 'orders_{}.csv'.format(date)
    dst = os.path.join(dts_dirname, dst_filename)
    if os.path.exists(dst) and os.path.isfile(dst):
        return dst
    df = pd.read_csv(src, parse_dates=['date'])
    df = df.groupby('email').count().sort_values('email')
    result = pd.DataFrame()
    result['email'] = df.index
    result['orders'] = df.date.values
    result.to_csv(dst, index=False)
    return dst


def upload_orders_to_s3(**context):
    bucket_name = 'korniichuk.demo'
    src = context['ti'].xcom_pull(task_ids='calc_orders')
    dst_dirname = 'workflow/output'
    dst_filename = os.path.basename(src)
    dst = os.path.join(dst_dirname, dst_filename)
    if not exists_in_s3(bucket_name, dst):
        s3 = boto3.resource('s3')
        s3.Bucket(bucket_name).upload_file(src, dst)
    return dst


default_args = {
    'owner': 'korniichuk',
    'start_date': datetime.datetime(2019, 8, 6)
}

with DAG('orders',
         default_args=default_args,
         schedule_interval='@daily') as dag:

    src = 's3://korniichuk.demo/workflow/input'
    dst = '/tmp/airflow-orders/gz'
    command = 'aws s3 sync {} {}'.format(src, dst)
    download_from_s3 = BashOperator(task_id='download_from_s3',
                                    bash_command=command)
    decompress = PythonOperator(task_id='decompress',
                                python_callable=decompress)
    preprocess_jsons = PythonOperator(task_id='preprocess_jsons',
                                      python_callable=preprocess_jsons,
                                      provide_context=True)
    merge_csvs = PythonOperator(task_id='merge_csvs',
                                python_callable=merge_csvs,
                                provide_context=True)
    upload_transactions_to_s3 = PythonOperator(
            task_id='upload_transactions_to_s3',
            python_callable=upload_transactions_to_s3,
            provide_context=True)
    calc_orders = PythonOperator(task_id='calc_orders',
                                 python_callable=calc_orders,
                                 provide_context=True)
    upload_orders_to_s3 = PythonOperator(task_id='upload_orders_to_s3',
                                         python_callable=upload_orders_to_s3,
                                         provide_context=True)
    path = '/tmp/airflow-orders'
    command = 'rm -rf {}'.format(path)
    clean = BashOperator(task_id='clean', bash_command=command)

download_from_s3 >> decompress >> preprocess_jsons >> merge_csvs
merge_csvs >> upload_transactions_to_s3
merge_csvs >> calc_orders >> upload_orders_to_s3
[upload_transactions_to_s3, upload_orders_to_s3] >> clean
