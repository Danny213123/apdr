"""Apache Airflow DAG — 'airflow' import maps to 'apache-airflow' on PyPI."""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks import FTPHook
from airflow.hooks.mysql_hook import MySqlHook
from datetime import datetime, timedelta
import codecs
import os
import logging

dag = DAG('my_dag', start_date=datetime(2020, 1, 1))
