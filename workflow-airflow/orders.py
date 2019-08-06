#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Version: 0.1a1

import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def example_func():
    pass


default_args = {
    'owner': 'korniichuk'
}

dag = DAG(
    dag_id='orders',
    schedule_interval='@daily',
    start_date=datetime.datetime(2019, 8, 5),
    default_args=default_args
)

task1 = DummyOperator(
    task_id='task1',
    dag=dag
)

task2 = PythonOperator(
    task_id='task2',
    dag=dag,
    default_args=default_args,
    python_callable=example_func
)

dag.set_dependency('task1', 'task2')
