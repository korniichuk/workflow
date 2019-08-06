#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Version: 0.1a2

import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def example_func():
    pass


default_args = {
    'owner': 'korniichuk',
    'start_date': datetime.datetime(2019, 8, 5)
}

with DAG('orders',
         default_args=default_args,
         schedule_interval='@daily') as dag:

    task1 = DummyOperator(task_id='task1')
    task2 = PythonOperator(task_id='task2',
                           python_callable=example_func)

task1 >> task2
