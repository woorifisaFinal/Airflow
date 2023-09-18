# Airflow
- dags 내에 있는 .py 파일 하나 하나를 dag라고 부른다.
- airflow는 workflow를 관리하는 tool이고 하나의 dag가 하나의 workflow다.
- workflow 내에서 아래와 같이 work(task)들을 정의해주면 된다.

```
    # 현재 정의된 work
    
    # stage1 추론
    run_stage1>> save_stage1_per_one_day
    # stage2 (포트폴리오 모델에 사용될 종가 야후 finance로 받아오기)
    save_close_per_one_day
    # 잘 save가 진행되었을 경우
    # RDS에 저장된 stage1 추론값과 종가들을 EC2 내에 파일(csv)로 저장해준다.
    [save_stage1_per_one_day, save_close_per_one_day] >> [get_close_for_portfolio, get_stage1_for_portfolio]
    # 잘 get했을 경우, 3개의 포트폴리오 모델을 추론하고 RDS에 저장해준다.
    [get_close_for_portfolio, get_stage1_for_portfolio] >> run_portfolio_model
```
![Airflow graph](https://airflow.apache.org/docs/apache-airflow/stable/_images/edge_label_example.png)
- 현재 정의된 work라고 나와 있는 것이 위와 같은 graph로 보여지므로 저걸 발표 PPT에 넣으면 좋다. [현재는 경로 문제 등 조금 손 봐야할 것이 있어 우리 task의 graph는 아직 얻을 수 없다. 저 사진은 웹에 돌아다니는 사진]
- dags내에 total_dag.py가 우리 model part의 최종 workflow 파일.
- 41번 줄부터 50번 줄까지는 RDS를 연결하는 코드,  누나의 everyday의 connection 코드와 동일한 기능
- DAG의 context관리자(with) 있는 곳이 실제 workflow 구현 부분. PythonOperator는 python 함수를 실행시키며 python_callable에 함수명을 넣어주면 된다.
-  bashoperator는 bash command 명령어를 실행시켜 준다. run_stage1 변수, 즉 stage1을 돌리는 부분이 핵심이다.
```
    bash_command="${AIRFLOW_HOME}/dags/stage1_predict.sh ",
```
- 여기서부터 stage1이 시작된다.
- dags 내에 stage1_predict.sh로 들어가보자
- export를 통해 환경변수를 설정한다.
- today는 date라는 리눅스 명령어를 통해 년,월, 일을 받아온다.
- output_collection에서는 각 13개의 자산을 추론한 결과를 모두 가져와서 하나의 csv파일로 생성 후 Airflow 경로("/opt/airflow/stage1/output/stage1_prediction.csv")에 저장한다.
이 결과를 total_dag.py의 read_csv_and_store_in_mysql 함수를 통해 RDS에 저장하는 것이다. ( save_stage1_per_one_day )
- total_dag.py의 read_close_and_store_in_csv 함수는 RDS에서 종가 데이터들을 가져와 Airflow환경(EC2)에 csv를 저장하는 함수다.
- 이후 python -m stage1.tools.infer에 각 13개의 자산에 맞는 환경설정 파일(.yml)과 today 오늘 날짜를 인자로 넣어준다.
- stage1.tools.infer.py의 34번줄 부터 42번줄은 argument와 환경설정을 처리하는 부분이다.
- infer함수는 분기에 의해 user_name별로 추론 방식이 정해져있다.
<br></br>
- infer함수 내에서 models의 메서드 형식으로 models.create_jw_model() 같은 함수를 어떻게 사용한 건지 확인하고 싶다면 stage1.models 폴더 내에 __init__.py 파일과 각 파일(ftse.py, jw_model.py 등) 내의 __all__ 변수를 확인하면 된다.
- hs와 jh의 데이터 처리는 stage1/data에 있는 preprocess.py에서 실행된다.
- 각 models들의 파일(ex. ftse.py, tw_lstm.py 등)에서 mode가 train인 것은 17-20 학습 코드, valid는 21,22 배치로 추론 코드, infer는 현재 날짜 받아와서 하나의 날짜 추론하는 (batch size는 1, lookback 사이즈는 50) 코드들이다.
- tw 모델. tw_lstm.py 4개의 자산(국채)이 추론된다. 모듈화하기에 용이하므로 bond라는 함수를 생성하여 입력값만 다르게 주면 추론하도록 설정해 있다.
- jh_rnn_model.py는 함수만 정의되어 있고 실제 추론은 infer.py에 정의되어 있다.
- jh를 제외한 모든 user의 실제 추론은 모두 stage1/models 폴더 내 파일에서 실행된다. [ jh의 경우, infer.py에서 실행 ]

