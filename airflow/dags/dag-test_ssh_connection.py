# from airflow.sdk import DAG
# from airflow.providers.ssh.operators.ssh import SSHOperator
# from datetime import datetime, timedelta

# default_args = {
#     'owner': 'huynnx',
#     'depends_on_past': False,
#     'retries': 1,
# }

# with DAG(
#     dag_id='ssh_test',
#     default_args=default_args,
#     start_date=datetime.today() - timedelta(days=1),
#     schedule=None,
# ) as dag:
#     ssh_task = SSHOperator(
#         task_id='run_remote_command',
#         ssh_conn_id='ssh_test_conn',
#         command='echo "Hello from remote server!"',
#     )
#     ssh_task