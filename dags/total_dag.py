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
import yfinance as yf
# import sys
# sys.path.append("/opt/airflow/model_backend")

# from CustomModel import custom_model
from .model_backend.AlphaModel import alpha_model
from .model_backend.blackLitterman import runBlack
import sys, time, json, logging


local_tz = pendulum.timezone("Asia/Seoul")
today = pendulum.now().date()
before_1_year = today - pd.Timedelta(weeks=52)
before_1_year = before_1_year.strftime('%Y-%m-%d')
today = today.strftime('%Y-%m-%d')

default_args = {
    'owner': 'joohyuk',  # 이 작업의 오너입니다. 보통 작업을 담당하는 사람 이름을 넣습니다.
    'start_date': datetime(2023, 9, 12),
    "schedule_interval":"*/2  * * * *", # 매일 6시 AM에 실행하도록 설정
    'tags': ['customDAG'],
    'retires': 1, # 실험 중이기에 재시도하지 않음
    'retry_delay': timedelta(minutes=5),
}
# retries : 5,
# 'on_failure_callback':alert.slack_fail_alert,
# 'on_success_callback':alert.slack_success_alert
# 성공/실패시 콜백 함수

connection=BaseHook.get_connection('AWS_RDB') #airflow에서 connection한 mysql id
database_username=connection.login
database_password=connection.password
database_ip = connection.host
database_port = connection.port
database_name = connection.schema

database_connection = f"mysql+pymysql://{database_username}:{database_password}@{database_ip}:{database_port}/{database_name}"

engine = create_engine(database_connection)

# 참조: https://velog.io/@moon_happy/Airflow-CSV%ED%8C%8C%EC%9D%BC%EC%9D%84-MySQL%EC%97%90-%EC%A0%81%EC%9E%AC
def read_csv_and_store_in_mysql():
    # CSV 파일을 읽어 DataFrame으로 변환합니다.
    # df = pd.read_csv(f"/opt/airflow/stage1/output/stage1_prediction_{today}.csv")
    df = pd.read_csv(f"/opt/airflow/stage1/output/stage1_prediction.csv")

    # 데이터를 MySQL의 새 테이블에 저장합니다. 기존에 있는 테이블이라면, 데이터를 덮어쓰거나 추가할 수 있습니다.
    df.to_sql(con=engine, name='stage1', if_exists='append', index=False)

def getClose():
    ##FOR PROJECT
    # now = datetime.datetime.now()
    # today = now.strftime("%Y-%m-%d")
    # tomorrow = now + datetime.timedelta(days=1)
    # tomorrow = tomorrow.strftime("%Y-%m-%d")
    ##FOR LOCAL
    # start='2017-01-01'
    # end='2023-08-31' 
    symbol = ['^IXIC','^FTSE','^N225','^STOXX50E','^KS11','^BVSP','^TWII','^BSESN', 'GC=F']
    col_list = ['us', 'uk', 'jp', 'euro', 'kor', 'ind', 'tw', 'br', 'gold'] 
    before_10_days = (pendulum.now().date()-pd.Timedelta(days=5)).strftime('%Y-%m-%d')
    df = yf.download(symbol, before_10_days, today)
    dfs = df['Close']
    rename_dict = {old_col: new_col for old_col, new_col in zip(dfs.columns, col_list)}
    # 마지막 하루만 넣어주기, 이전 날이 휴일일 수도 있으니깐. 10일 가져옴
    dfs = dfs.rename(columns=rename_dict).tail(1)

    dfs.to_sql(name='symboltest', con=engine, index=True, if_exists='append',) # schema='woorifisa'
    # connection.commit()
    return 0

def read_close_and_store_in_csv():
    """
    1년 전부터 종가 데이터들 취합
    """
    table = "symboltest"
    
    query = f"SELECT * FROM {table} BETWEEN {before_1_year} AND {today}"
    # 데이터를 MySQL의 새 테이블에 저장합니다. 기존에 있는 테이블이라면, 데이터를 덮어쓰거나 추가할 수 있습니다.
    df = pd.read_sql(sql=query, con=engine)
    # 종가들 내 공휴일을 이유로(?) 결측값 있는 것 모두 interpolate로 채워주기 (처음과 끝에 있는 결측값들은 채워지지 않는다.)
    df = df.interpolate()
    df.to_csv("/opt/airflow/data/close.csv")

def read_stage1_and_store_in_csv():
    """
    30(영업)일 전부터 stage1 예측값 불러오기
    """
    table = "stage1"
    extract_num = 30 # 넉넉하게 30개 저장해두기
    query = f"SELECT * FROM {table} LIMIT {extract_num}"
    # 데이터를 MySQL의 새 테이블에 저장합니다. 기존에 있는 테이블이라면, 데이터를 덮어쓰거나 추가할 수 있습니다.
    df = pd.read_sql(sql=query, con=engine)
    df = df.interpolate()
    df.to_csv("/opt/airflow/data/stage1_result.csv")

def saveTodayPortfolio():
    ##FOR PROJECT
    # now = datetime.datetime.now()
    # today = now.strftime("%Y-%m-%d")
    ##FOR LOCAL
    # today = '2022-08-01'


    a1, a2 = alpha_model()


    b1, b2 = runBlack()
    b1_ = json.loads(b1)
    b2_ = json.loads(b2)

    # c1, c2 = custom_model(today)


    a1 = pd.DataFrame(a1).T
    a1 = a1.astype('float')
    a1['type'] = "A/안정형"
    a1['type'] = a1['type'].astype(str)

    a2 = pd.DataFrame(a2).T
    a2 = a2.astype('float')
    a2['type'] = 'A/공격형'
    a2['type'] = a2['type'].astype(str)

    b1 = pd.json_normalize(b1_)
    b1 = b1.astype('float')
    b1['type'] = 'B/공격형'
    b1['type'] = b1['type'].astype(str)

    b2 = pd.json_normalize(b2_)
    b2 = b2.astype('float')
    b2['type'] = 'B/안정형'
    b2['type'] = b2['type'].astype(str)

    # c1 = c1.astype('float')
    # c1['type'] = "C/안정형"
    # c1['type'] = c1['type'].astype(str)

    # c2 = c2.astype('float')
    # c2['type'] = 'C/공격형'
    # c2['type'] = c2['type'].astype(str)


    # df = pd.concat([b1, b2, a1, a2, c1, c2])
    df = pd.concat([b1, b2, a1, a2])
    df['date'] = today
    df.to_sql(name='portfolio', con=engine, index=False, if_exists='append',  schema='woorifisa')
    return 0

with DAG(dag_id='total_dag', default_args=default_args, catchup=False) as dag:

    run_stage1 = BashOperator(
        task_id="run_stage1_for_all_assest",
        bash_command="${AIRFLOW_HOME}/dags/stage1_predict.sh ",
        retries=3, # 이 태스크가 실패한 경우, 3번 재시도 합니다.
        retry_delay=timedelta(minutes=1), # 재시도하는 시간 간격은 1분입니다. 
    )

    save_stage1_per_one_day = PythonOperator(
	task_id='save_stage1_per_one_day', # read_csv_and_store_in_mysql
	python_callable=read_csv_and_store_in_mysql,
	dag=dag,
    )

    save_close_per_one_day = PythonOperator(
	task_id='save_close_per_one_day',
	python_callable=getClose,
	dag=dag,
    )


    get_close_for_portfolio = PythonOperator(
	task_id='get_close_for_portfolio',
	python_callable=read_close_and_store_in_csv,
	dag=dag,
    )

    get_stage1_for_portfolio = PythonOperator(
	task_id='get_stage1_for_portfolio',
	python_callable=read_stage1_and_store_in_csv,
	dag=dag,
    )

    run_portfolio_model = PythonOperator(
	task_id='run_portfolio_model',
	python_callable=saveTodayPortfolio,
	dag=dag,
    )
    
    # run_portfolio_model = BashOperator(
    #     task_id='run_portfolio_model',
    #     bash_command ='python /opt/airflow/model_backend/everyday.py'
    # )

    # stage1 돌리기 >> stage1 추론 결과 RDS 저장, 종가 데이터 RDS 저장 >> 
    run_stage1 >> [save_stage1_per_one_day, save_close_per_one_day] >> \
        [get_close_for_portfolio, get_stage1_for_portfolio]\
    >> run_portfolio_model
    