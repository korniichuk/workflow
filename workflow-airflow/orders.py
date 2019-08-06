#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Version: 0.1a3

import datetime
import os
from subprocess import check_call

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


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


default_args = {
    'owner': 'korniichuk',
    'start_date': datetime.datetime(2019, 8, 5)
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

download_from_s3 >> decompress
