"""DAG to trigger Spark job on WSL via SSH."""
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

with DAG(
    'grocery_data_load',
    start_date =  datetime(2026, 4, 11),
    schedule = None,
    catchup = False
) as dag:

    run_spark = SSHOperator(
        task_id = 'run_spark',
        ssh_conn_id =  'wsl_ssh_conn',
        command='''
            export SPARK_HOME=/opt/spark
            export PATH=$PATH:$SPARK_HOME/bin
            spark-submit \
            --jars /home/deepak/spark_jars/postgresql-42.7.2.jar \
            --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.41.0 \
            /home/deepak/projects/sparkwork/spark_bq_json/pg_grocery_to_bq.py
        ''',
        get_pty=True
    )
