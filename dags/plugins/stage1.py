import pandas as pd

def getPrediction():
    data = pd.read_csv("/opt/airflow/data/stage1_result.csv")
    data.set_index("date", inplace=True)


    # # 기간 설정
    # lookback_period_months = 1  # 1개월치 데이터를 사용하고 싶을 때 설정

    # # 기간 계산
    # end_date = pd.Timestamp(2022,12,31)
    # start_date = end_date - pd.DateOffset(months=lookback_period_months)


    # # 데이터 추출
    # df_1_months = data.loc[(data.index > start_date.strftime('%Y-%m-%d')) & (data.index <= end_date.strftime('%Y-%m-%d'))]
    # return df_1_months

    # 30일만 저장해 두기 때문에 바로 넣어도 될 듯
    return data

