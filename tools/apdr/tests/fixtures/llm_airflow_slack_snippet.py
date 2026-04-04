"""Airflow plugin with Slack integration.
airflow -> apache-airflow, slackclient -> slackclient.
titan.utils is a LOCAL module — skip it."""
import logging
from airflow.operators.python_operator import PythonOperator
from airflow.plugins_manager import AirflowPlugin
from slackclient import SlackClient
from titan.utils import config
