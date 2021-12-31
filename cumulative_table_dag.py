from airflow import DAG
from airflow.contrib.operators.presto_operator import PrestoOperator

default_args = {
    'owner': 'Data with Zach',
    # Very important to have your cumulative DAGs be set `depends_on_past = True`
    'depends_on_past': True,
    'email': ['airflow@example.com'],
}
with DAG(
    'cumulative_table_example_dag',
    default_args=default_args,
    description='A simple cumulative table example',
    schedule_interval='@daily',
    start_date='2022-01-01',
    catchup=False,
    tags=['data with zach, cumulative table example'],
) as dag:
    daily_task = PrestoOperator(
        task_id='compute_active_user_daily',
        sql='queries/active_users_daily_populate.sql'
    )

    cumulative_task = PrestoOperator(
        task_id='compute_active_users_cumulated',
        sql='queries/active_users_cumulated_populate.sql'
    )

    # the daily task has to run before the cumulative task
    daily_task >> cumulative_task