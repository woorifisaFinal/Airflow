


from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.mysql.operators.mysql import MySqlOperator


import pendulum # python의 datetime을 좀더 편하게 사용할 수 있게 돕는 모델
from datetime import datetime,  timedelta

local_tz = pendulum.timezone("Asia/Seoul")

def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('AWS_S3')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)


default_args = {
    'owner': 'joohyuk',  # 이 작업의 오너입니다. 보통 작업을 담당하는 사람 이름을 넣습니다.
    # 'schedule_interval': '@daily',
    'start_date': datetime(2023, 9, 12),
    "schedule_interval":"*/2  * * * *", # 매일 06:00에 실행합니다.
    'tags': ['temp'],
}
# start_date=datetime(2023, 8, 7, hour=12, minute=30),
    # start_date=days_ago(2), # DAG 정의 기준 2일 전부터 시작합니다.
	 # DAG 정의 기준 시간부터 시작합니다
    # start_date=airflow.utils.dates.days_ago(14),  
# 			# 스케쥴의 간격과 함께 DAG 첫 실행 시작 날짜를 지정해줍니다.
#       # 주의: 1월 1일에 DAG를 작성하고 자정마다 실행하도록 schedule_interval을 지정한다면 태스크는 1월 2일 자정부터 수행됩니다	
#     end_date=datetime(year=2023, month=8, day=19),
		
# 		schedule_interval="@daily", # DAG 실행 간격 - 매일 자정에 실행

# default_args = {
#     'depends_on_past': False,
#     'retires': 1,
#     'retry_delay': timedelta(minutes=5)
# }

with DAG(dag_id='stage1_predict_rds', default_args=default_args, catchup=False) as dag:

    t1 = BashOperator(
        task_id="all_assest",
        # bash_command="/opt/airflow/stage1/tools/stage1_predict.sh ",
        # bash_command="../stage1/tools/stage1_predict.sh ",
        # bash_command="${AIRFLOW_HOME}/stage1/tools/stage1_predict.sh ",
        bash_command="${AIRFLOW_HOME}/dags/stage1_predict.sh ",
        
        retries=3, # 이 태스크가 실패한 경우, 3번 재시도 합니다.
        retry_delay=timedelta(minutes=1), # 재시도하는 시간 간격은 1분입니다.
        
    )

    # 3) 최종적으로 태스크들 간의 실행 순서를 결정
    # t1 실행 후 t2를 실행합니다.
    # t1 >> t2
    t1