#Dockerfile-airflow
from apache/airflow:2.9.3

#Switch to airflow user 
user airflow

run pip install --no-cache-dir dbt-core dbt-snowflake