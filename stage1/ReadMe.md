
# 0907 정리내용
- SM
    - **진행 완료**
        - 지수별(ftse, nikkei, nasdaq) LSTM만 train, val로 분리 ( xgb는 X)
        - 경로 변경 (opj(cfg.base.output_dir, data_dir 등))
        - 계속 지수이름으로 (모델명, task_name 등에서) 사용하다가 출력 나타낼 때만 약속된 약어(jp 등)로 사용 (model.py에서 추론 부분)
    - **향후**
        - ETL 자동화를 위해 raw 데이터로부터 전처리하는 코드 필요 (DataPreprocress Class에 저장해 놓을 계획)
        - raw 데이터 출처 필요 ( 크롤링으로 매일 특정시간에 받아올 계획.. 진행한다면.. )
    
- TW
    - **진행 완료**
        - cfg.data.lookahead_window 활용 30일 후 예측하도록
        - bond 함수로 4개 한 번에 진행.
        - 추론 날짜 받아오는 data_sequences_2021, data_sequences_2022 생성 (create sequence 부분)
    - **향후**

# 목표 
1. ipynb 파일들을 py 파일로 바꾸는 것 **필수**
2. 파일 내 핵심 코드들을 분리하여 저장하는 것  **->현재 위치**
    - model 관련 코드는 models, 학습, 추론하는 코드는 tools, data들은 data 폴더 안에, data 전처리 하는 코드는 data 폴더 내 preprocess.py에 
    - data 전처리하는 코드 모아놓는 preprocess 폴더를 만드는 것도 이야기해볼 필요 있음
3. 분리된 코드들을 하나의 함수 or 하나의 class를 통해 불러와 사용 
    - ex..특정 모델 호출 함수를 만들어, user_name과 model_name만 넣어주면 모델을 불러와주는 함수?
4. 자동화하는 것
    - 수동으로 데이터 다운로드했던 것을 크롤링 코드로 자동화한다면 좋겠다.
    - Airflow와 Jenkins를 활용해본다면 좋겠다. 

# configuration 설명
- config : 모든 설정이 저장되어 있는 곳, train할 때, infer할 때 나눠져 있다.
    - infer yml은 매일 하루씩 추론할 때 해당 파일의 start_date, end_date만 달라지게 해주고 추론되도록.. 할 수 있지 않을까라는 생각에서 시작 (-> 최선이 아닐 수 있으니 모두의 지혜가 필요합니다.)
    - model_name을 lstm 혹은 xgboost로 바꾸면 해당 모델이 돌아가도록
    - output_dir : 학습된 모델의 가중치와 예측 결과가 저장되는 곳
- configuration 파일(.yml) 명명 규칙
    - 자산명_모델명_학습추론여부.yml (ex. ks_lstm_train.yml -> 코스피를 lstm으로 학습하는 설정)
- configuration 파일 내 경로는 stage1이 시작 경로로 되어 있다. [AWS에서 사용할 때 달라진다면 yml에서만 바꿔주면 된다.]
- configuration 파일 내 task_name == index_name..
- 가능하면 loockback_window, lookahead_window에 값을 입력하면 해당 일수만큼 학습하거나 추론하도록 설정해주시면 좋겠습니다. (lookback : 몇 일 동안을 보고 예측할지, lookahead :몇 일 후를 예측할지)


# 공통 요구사항 (생각해볼 부분)
- 추론 때 사용할 scaler를 저장해 두면 좋겠다는 생각 
    - [22년이 아닌 23년, 즉 새로운 데이터들로 추론을 매일 하도록 할 것이라면 필요 ]
- 각자 겹치는 코드들은 가장 깔끔한 한 명의 코드를 같이 사용하면 좋겠습니다.
    - ex. investing .com에서 받아온 데이터들의 공통 전처리 사항, 점(.) 없애기, % 없애기 등
- 
# 이유
- output.txt, data.txt 존재 이유 : 아무 의미 없이 폴더 구조 유지하기 위해 내용물을 단순 추가해놓은 것
- tools에 infer와 train을 나누어 놓은 이유 : 매일 하루씩 추론할 때, 학습 과정 없이 추론만 해야 한다. 따라서, 학습 없이 추론할 수 있는 코드도 필요하다고 생각.
    - 코드가 학습 후 추론 가능하도록 코드가 생성되어 있는데 추론 자동화를 위해서 분리해주신다면 좋겠다는 생각입니다. [ex. 학습 때 생성한 scaler 저장]
    - 저는 scaler를 output dir에 저장해 놓았습니다.
- utilty 폴더 존재 이유 : 학습, 추론에 주도적인 역할은 아닌, 여러 잡다한 코드들을 모아놓는 곳, logging, seed 설정 코드 등 (저는 metric 코드도 모아놓았습니다.)
- models에 user별 폴더가 존재하지 않는 이유 : 처음에는 lstm.py, xgboost.py로 해서 user_name과 index_name(task_name)에 맞춰 모델을 생성해주는 걸로 생각했습니다. 그러자니 시간이 오래 걸릴 것 같아서 우선은 각 파일별로 앞에 user_name을 적어주었습니다. -> 따라서, 같이 생각해볼 부분입니다.
 
# 진행 상황
- BG
    - 폴더 경로 생성 완료
- HS
    - 폴더 경로 생성 완료
- JH
    data,model, tools 제작 완료
- JW
    - lstm코드 추가 완료 (xgboost 필요)
- SM
    - 폴더 경로 생성, model 파일만(내용x) 생성 완료
- TW
    - 폴더 경로 생성 완료
- 공통
    - 각 자산의 예측 output을 모아 하나의 csv 파일을 만드는 코드 생성 완료
