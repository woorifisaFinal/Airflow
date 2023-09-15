from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
# from airflow.providers.mysql.operators.mysql import MySqlOperator
import pendulum # python의 datetime을 좀더 편하게 사용할 수 있게 돕는 모델
from datetime import datetime,  timedelta
import pandas as pd
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine
import pymysql

local_tz = pendulum.timezone("Asia/Seoul")
today = pendulum.now().date().strftime('%Y-%m-%d')

default_args = {
    'owner': 'joohyuk',  # 이 작업의 오너입니다. 보통 작업을 담당하는 사람 이름을 넣습니다.
    'start_date': datetime(2023, 9, 12),
    "schedule_interval":"*/2  * * * *", # 매일 06:00에 실행합니다.
    'tags': ['temp'],
}

# 참조: https://velog.io/@moon_happy/Airflow-CSV%ED%8C%8C%EC%9D%BC%EC%9D%84-MySQL%EC%97%90-%EC%A0%81%EC%9E%AC
def read_csv_and_store_in_mysql():
    # CSV 파일을 읽어 DataFrame으로 변환합니다.
    df = pd.read_csv(f"/opt/airflow/stage1/output/stage1_prediction_22.csv")
    connection=BaseHook.get_connection('AWS_RDB') #airflow에서 connection한 mysql id
    database_username=connection.login
    database_password=connection.password
    database_ip = connection.host
    database_port = connection.port
    database_name = connection.schema
    
    database_connection = f"mysql+pymysql://{database_username}:{database_password}@{database_ip}:{database_port}/{database_name}"

    engine = create_engine(database_connection)

    # 데이터를 MySQL의 새 테이블에 저장합니다. 기존에 있는 테이블이라면, 데이터를 덮어쓰거나 추가할 수 있습니다.
    df.to_sql(con=engine, name='stage1', if_exists='append', index=False)


with DAG(dag_id='stage1_predict_batch_22_rds_direct_batch', default_args=default_args, catchup=False) as dag:

    t1 = BashOperator(
        task_id="all_assest",
        bash_command="${AIRFLOW_HOME}/dags/stage1_predict_22_batch.sh ",
        retries=3, # 이 태스크가 실패한 경우, 3번 재시도 합니다.
        retry_delay=timedelta(minutes=1), # 재시도하는 시간 간격은 1분입니다.
        
    )

    t2 = PythonOperator(
	task_id='read_csv_and_store_in_mysql',
	python_callable=read_csv_and_store_in_mysql,
	dag=dag,
)

    # 3) 최종적으로 태스크들 간의 실행 순서를 결정
    # t1 실행 후 t2를 실행합니다.
    # t1 >> t2
    t1 >> t2