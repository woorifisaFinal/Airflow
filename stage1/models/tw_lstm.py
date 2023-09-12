from os.path import join as opj
import pickle

import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
import matplotlib.pyplot as plt

##### lookahead_window 50개
__all__ = ['bond_short', 'bond_long', "us_bond_long", "us_bond_short"]

# 시계열 데이터 생성 함수 수정
def create_sequence_data(data, sequence_length, lookahead_window):
    sequences = []
    # 추론 날짜
    dates_list = []
    for i in range(len(data) - sequence_length-lookahead_window + 1):
        sequence = data.iloc[i:i + sequence_length][['종가', '시가', '저가', '변동률']].values  # 수익률 제외
       
        
        if i + sequence_length < len(data):
            target = data.iloc[i + sequence_length+lookahead_window-1]['수익률']  # 수익률을 타겟으로 변경
            sequences.append((sequence, target))
            # i+sequence_length가 중간값
            dates = data.iloc[i + sequence_length]['날짜']#.values[0] # timestamp라서 문제 생긴다면 .dt.strftime('%Y-%m-%d') 
            dates_list.append(dates)
    return sequences, dates_list


def bond(cfg, file_name, country):
    if cfg.base.mode == 'train':
        # 데이터 로드 및 전처리
        data = pd.read_csv(opj(cfg.base.data_dir, file_name), encoding='cp949')
        data['날짜'] = pd.to_datetime(data['날짜'])
        data = data.sort_values(by='날짜')
        data = data[['날짜', '종가', '시가', '저가', '변동률', '수익률']]
        if country =='kor':
            data['종가'] = data['종가'].str.replace(',', '').astype(float)
            data['시가'] = data['시가'].str.replace(',', '').astype(float)
            data['저가'] = data['저가'].str.replace(',', '').astype(float)
            data['변동률'] = data['변동률'].str.replace('%', '').astype(float) / 100.0
            data['수익률'] = data['수익률'].str.replace('%', '').astype(float) / 100.0

        # 데이터 스케일링
        scaler_closing = MinMaxScaler()
        scaler_opening = MinMaxScaler()
        scaler_low = MinMaxScaler()
        scaler_volatility = MinMaxScaler()
        scaler_return = MinMaxScaler()

        data['종가'] = scaler_closing.fit_transform(data[['종가']])
        data['시가'] = scaler_opening.fit_transform(data[['시가']])
        data['저가'] = scaler_low.fit_transform(data[['저가']])
        data['변동률'] = scaler_volatility.fit_transform(data[['변동률']])
        data['수익률'] = scaler_return.fit_transform(data[['수익률']])
        
        for scaler, name in zip([scaler_closing, scaler_opening, scaler_low, scaler_volatility, scaler_return], ['close','open','low','volatility','return']):
            with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_{name}_scaler.pkl"), 'wb') as f:
                pickle.dump(scaler, f)
        


        sequence_length = 50  # 시퀀스 길이 설정
        
        train_data = data[(data['날짜'].dt.year >= 2017) & (data['날짜'].dt.year <= 2021)]
        # data_list 학습할 때는 필요 없잖아..
        train_sequences, _ = create_sequence_data(train_data, sequence_length, cfg.data.lookahead_window)
        X_train = np.array([sequence for sequence, target in train_sequences])
        y_train = np.array([target for sequence, target in train_sequences])

        model = Sequential([
        LSTM(50, activation='relu', input_shape=(sequence_length, 4)),# 4개 변수를 입력으로 설정
        Dense(1)
        ])

        model.compile(optimizer='adam', loss='mse')

        # 모델 학습
        model.fit(X_train, y_train, epochs=50, batch_size=32, verbose=2)

        model.save(opj(cfg.base.output_dir,f"{cfg.base.task_name}_lstm.h5"))

        # # 2022년 데이터로 예측 수행
        # test_data_2022 = data[data['날짜'].dt.year == 2022]
        # test_sequences_2022 = create_sequence_data(test_data_2022, sequence_length, cfg.data.lookahead_window)
        # X_test_2022 = np.array([sequence for sequence, target in test_sequences_2022])
        # y_test_2022 = np.array([target for sequence, target in test_sequences_2022])
        # predictions_2022 = model.predict(X_test_2022)

        # # 예측 결과 시각화
        # plt.figure(figsize=(12, 6))
        # plt.plot(np.arange(len(y_test_2022)), y_test_2022, label='Real data (2022)')
        # plt.plot(np.arange(len(y_test_2022)), predictions_2022, label='Forecast data (2022)')
        # plt.xlabel('days')
        # plt.ylabel('return')
        # plt.title('LSTM Forecast data for 2022')
        # plt.legend()
        # plt.xticks(rotation=45)
        # plt.show()

        # RMSE 계산
        # rmse_2022 = np.sqrt(mean_squared_error(y_test_2022, predictions_2022))
        # print(f'Root Mean Squared Error for 2022: {rmse_2022}')
    elif cfg.base.mode=='valid':
        # 데이터 로드 및 전처리
        data = pd.read_csv(opj(cfg.base.data_dir, file_name), encoding='cp949')
        data['날짜'] = pd.to_datetime(data['날짜'])
        data = data.sort_values(by='날짜')
        data = data[['날짜', '종가', '시가', '저가', '변동률', '수익률']]
        if country =='kor':
            data['종가'] = data['종가'].str.replace(',', '').astype(float)
            data['시가'] = data['시가'].str.replace(',', '').astype(float)
            data['저가'] = data['저가'].str.replace(',', '').astype(float)
            data['변동률'] = data['변동률'].str.replace('%', '').astype(float) / 100.0
            data['수익률'] = data['수익률'].str.replace('%', '').astype(float) / 100.0
        
        # 데이터 스케일링
        # with open(opj(cfg.base.output_dir, f"close_scaler.pkl"), 'rb') as f:
        #     scaler_closing = pickle.load(f)
        # with open(opj(cfg.base.output_dir, f"open_scaler.pkl"), 'rb') as f:
        #     scaler_opening = pickle.load(f)
        # with open(opj(cfg.base.output_dir, f"low_scaler.pkl"), 'rb') as f:
        #     scaler_low = pickle.load(f)
        # with open(opj(cfg.base.output_dir, f"low_scaler.pkl"), 'rb') as f:
        #     scaler_low = pickle.load(f)
        # with open(opj(cfg.base.output_dir, f"volatility_scaler.pkl"), 'rb') as f:
        #     scaler_volatility = pickle.load(f)         
        # with open(opj(cfg.base.output_dir, f"return_scaler.pkl"), 'rb') as f:
        #     scaler_return = pickle.load(f)

        # 데이터 스케일링
        with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_close_scaler.pkl"), 'rb') as f:
            scaler_closing = pickle.load(f)
        with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_open_scaler.pkl"), 'rb') as f:
            scaler_opening = pickle.load(f)
        with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_low_scaler.pkl"), 'rb') as f:
            scaler_low = pickle.load(f)
        with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_low_scaler.pkl"), 'rb') as f:
            scaler_low = pickle.load(f)
        with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_volatility_scaler.pkl"), 'rb') as f:
            scaler_volatility = pickle.load(f)         
        with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_return_scaler.pkl"), 'rb') as f:
            scaler_return = pickle.load(f)

        data['종가'] = scaler_closing.transform(data[['종가']])
        data['시가'] = scaler_opening.transform(data[['시가']])
        data['저가'] = scaler_low.transform(data[['저가']])
        data['변동률'] = scaler_volatility.transform(data[['변동률']])
        data['수익률'] = scaler_return.transform(data[['수익률']])

        from keras.models import load_model
        model = load_model(opj(cfg.base.output_dir,f"{cfg.base.task_name}_lstm.h5"))
        sequence_length = 50
        # 2021년 데이터로 예측 수행
        test_data_2021 = data[data['날짜'].dt.year == 2021]
        test_sequences_2021, data_sequences_2021 = create_sequence_data(test_data_2021, sequence_length, cfg.data.lookahead_window)
        X_test_2021 = np.array([sequence for sequence, target in test_sequences_2021])
        y_test_2021 = np.array([target for sequence, target in test_sequences_2021])
        predictions_2021 = model.predict(X_test_2021)


        # 2022년 데이터로 예측 수행
        test_data_2022 = data[data['날짜'].dt.year == 2022]
        test_sequences_2022, data_sequences_2022 = create_sequence_data(test_data_2022, sequence_length, cfg.data.lookahead_window)
        X_test_2022 = np.array([sequence for sequence, target in test_sequences_2022])
        y_test_2022 = np.array([target for sequence, target in test_sequences_2022])
        predictions_2022 = model.predict(X_test_2022)

        
        # with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_21.pkl"), 'wb') as f:
        #     pickle.dump(predictions_2021.reshape(-1,), f)

        # with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_22.pkl"), 'wb') as f:
        #     pickle.dump(predictions_2022.reshape(-1,), f)


        # 결과 저장
        pd.DataFrame(data={"date":data_sequences_2021, cfg.base.task_name:predictions_2021.reshape(-1,)}).to_csv(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_21.csv"), index=False)

        pd.DataFrame(data={"date":data_sequences_2022, cfg.base.task_name:predictions_2022.reshape(-1,)}).to_csv(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_22.csv"), index=False)
    else:
        # 데이터 로드 및 전처리
        data = pd.read_csv(opj(cfg.base.data_dir, file_name), encoding='cp949')
        data['날짜'] = pd.to_datetime(data['날짜'])
        data = data.sort_values(by='날짜')

        
        if (data['날짜']==cfg.base.base_date).sum()==0:
            # 존재하지 않는 날. 휴장
            pd.DataFrame(data={"date":cfg.base.base_date, cfg.base.task_name:np.NaN},index=[0]).to_csv(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_{cfg.base.base_date}.csv"), index=False)
            return 
        data = data[data['날짜']<cfg.base.base_date].tail(50)
            
        data = data[['날짜', '종가', '시가', '저가', '변동률', '수익률']]
        if country =='kor':
            data['종가'] = data['종가'].str.replace(',', '').astype(float)
            data['시가'] = data['시가'].str.replace(',', '').astype(float)
            data['저가'] = data['저가'].str.replace(',', '').astype(float)
            data['변동률'] = data['변동률'].str.replace('%', '').astype(float) / 100.0
            data['수익률'] = data['수익률'].str.replace('%', '').astype(float) / 100.0
        
        # 데이터 스케일링
        with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_close_scaler.pkl"), 'rb') as f:
            scaler_closing = pickle.load(f)
        with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_open_scaler.pkl"), 'rb') as f:
            scaler_opening = pickle.load(f)
        with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_low_scaler.pkl"), 'rb') as f:
            scaler_low = pickle.load(f)
        with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_low_scaler.pkl"), 'rb') as f:
            scaler_low = pickle.load(f)
        with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_volatility_scaler.pkl"), 'rb') as f:
            scaler_volatility = pickle.load(f)         
        with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_return_scaler.pkl"), 'rb') as f:
            scaler_return = pickle.load(f)

        data['종가'] = scaler_closing.transform(data[['종가']])
        data['시가'] = scaler_opening.transform(data[['시가']])
        data['저가'] = scaler_low.transform(data[['저가']])
        data['변동률'] = scaler_volatility.transform(data[['변동률']])
        data['수익률'] = scaler_return.transform(data[['수익률']])

        from keras.models import load_model
        model = load_model(opj(cfg.base.output_dir,f"{cfg.base.task_name}_lstm.h5"))

        X = np.array([data[['종가', '시가', '저가', '변동률']].values])
        test_pred = model.predict(X)

        # 결과 저장
        # import pickle
        pd.DataFrame(data={"date":cfg.base.base_date, cfg.base.task_name:test_pred.reshape(-1,)}).to_csv(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_{cfg.base.base_date}.csv"), index=False)

def bond_short(cfg):
    bond(cfg, '3년국채 데이터17_22.csv', "kor")

def bond_long(cfg):
    bond(cfg, '한국10 병합.csv', "us")

def us_bond_short(cfg):
    bond(cfg, '미국3 병합.csv', "us")

def us_bond_long(cfg):
    bond(cfg, '미국10 병합.csv', "us")
