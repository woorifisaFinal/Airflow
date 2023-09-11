from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook



def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('AWS_S3')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)


default_args = {
    'owner': 'joohyuk',  # 이 작업의 오너입니다. 보통 작업을 담당하는 사람 이름을 넣습니다.
    'schedule_interval': '@daily',
    'start_date': datetime(2022, 7, 30),
    'tags': ['temp'],
}
# default_args = {
#     'depends_on_past': False,
#     'retires': 1,
#     'retry_delay': timedelta(minutes=5)
# }

with DAG(dag_id='pwd_print', default_args=default_args, catchup=False) as dag:
  t1 = BashOperator(
      task_id="print_pwd",
      bash_command="pwd",
      owner="fisa", # 이 작업의 오너입니다. 보통 작업을 담당하는 사람 이름을 넣습니다.
      retries=3, # 이 태스크가 실패한 경우, 3번 재시도 합니다.
      retry_delay=timedelta(minutes=1), # 재시도하는 시간 간격은 1분입니다.
  )
