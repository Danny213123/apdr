"""Airflow DAG using pyodbc for database access.
airflow -> apache-airflow, pyodbc -> pyodbc (needs system libodbc)."""
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks import FTPHook
from airflow.hooks.mysql_hook import MySqlHook
from datetime import datetime, timedelta
import pyodbc
