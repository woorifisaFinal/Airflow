import pandas as pd
from sklearn.preprocessing import StandardScaler
import numpy as np
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from tensorflow.keras.optimizers import Adam
from pickle import dump
import joblib
from xgboost import XGBRegressor
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import train_test_split

from os.path import join as opj

__all__ = ["create_jw_lstm", "create_jw_xgboost"]

def create_jw_lstm(cfg):
# 데이터 불러오기
    raw_train = pd.read_csv(opj(cfg.base.data_dir, "adj_raw_train.csv"))
    raw_train['date'] = pd.to_datetime(raw_train['date'])
    dates = pd.to_datetime(raw_train['date'])



    target_year=2021
    train = raw_train[raw_train['date'].dt.year<target_year]
    validation = raw_train[raw_train['date'].dt.year==target_year]
    test = raw_train[raw_train['date'].dt.year==(target_year+1)]
    raw_train.set_index("date",inplace=True)


    # 데이터 내보내기
    # train.to_csv("jw_train.csv")
    # validation.to_csv("jw_validation.csv")
    # test.to_csv("jw_test.csv")



    # train 데이터 전처리
    cols = list(raw_train)[0:9]
    stock_data = raw_train[cols].astype(float)

    n_train = len(train)
    n_validation = n_train + len(validation)
    n_test = n_validation

    if cfg.base.mode=='train':
        scaler = StandardScaler()
        scaler = scaler.fit(stock_data[:n_validation])
        stock_data_scaled = scaler.transform(stock_data[:n_validation])
        stock_data_scaled_test = scaler.transform(stock_data[n_validation:])


        dump(scaler, open(opj(cfg.base.output_dir,'lstm_scaler.pkl'), 'wb'))
    else:
        import pickle
        with open(opj(cfg.base.output_dir,'lstm_scaler.pkl'), 'rb') as f:
            scaler = pickle.load(f)
        stock_data_scaled = scaler.transform(stock_data[:n_validation])
        stock_data_scaled_test = scaler.transform(stock_data[n_validation:])


    stock_data_target = raw_train[["target"]]


    # split- X
    train_data_scaled = stock_data_scaled[0: n_train]
    train_dates = dates[0: n_train]

    val_data_scaled = stock_data_scaled[n_train: n_validation]
    val_dates = dates[n_train: n_validation]

    test_data_scaled = stock_data_scaled_test
    test_dates = dates[n_test:]

    # split- y
    train_data_test_scaled = stock_data_target[0: n_train]
    train_dates = dates[0: n_train]

    val_data_test_scaled = stock_data_target[n_train: n_validation]
    val_dates = dates[n_train: n_validation]

    test_data_test_scaled = stock_data_target[n_test:]
    test_dates = dates[n_test:]




    # data reformatting for LSTM
    pred_days = 30
    seq_len = 50  
    input_dim = 10  

    trainX = []
    trainY = []
    valX = []
    valY = []
    testX = []
    testY = []

    # 추론 날짜 (base_date 출력을 위한)
    val_dates_for_infer = []
    test_dates_for_infer = []
    # val_dates, test_dates는 pd.Series로 되어 있고 numpy datetime으로 되어 있다.
    val_dates = val_dates.astype('string')
    test_dates = test_dates.astype('string')

    for i in range(seq_len, n_train-pred_days +1):
        trainX.append(train_data_scaled[i - seq_len:i, 0:train_data_scaled.shape[1]])
        trainY.append(train_data_test_scaled[i + pred_days - 1:i + pred_days].values)

    for i in range(seq_len, len(val_data_scaled)-pred_days +1):
        valX.append(val_data_scaled[i - seq_len:i, 0:val_data_scaled.shape[1]])
        valY.append(val_data_test_scaled[i + pred_days - 1:i + pred_days].values)
        # base_dates추가
        val_dates_for_infer.append(val_dates[i + pred_days - 1:i + pred_days].values[0])

    for i in range(seq_len, len(test_data_scaled)-pred_days +1):
        testX.append(test_data_scaled[i - seq_len:i, 0:test_data_scaled.shape[1]])
        testY.append(test_data_test_scaled[i + pred_days - 1:i + pred_days].values)
        # base_dates추가
        test_dates_for_infer.append(test_dates[i + pred_days - 1:i + pred_days].values[0])

    trainX, trainY = np.array(trainX), np.array(trainY)
    valX, valY = np.array(valX), np.array(valY)
    testX, testY = np.array(testX), np.array(testY)
    validation_data = (valX,valY)



    model = Sequential()
    model.add(LSTM(64, input_shape=(trainX.shape[1], trainX.shape[2]),
                return_sequences=True))
    model.add(LSTM(32, return_sequences=False))
    model.add(Dense(trainY.shape[1]))

    if cfg.base.mode == "train":
        learning_rate = 0.001
        optimizer = Adam(learning_rate=learning_rate)
        model.compile(optimizer=optimizer, loss='mse')



        model.fit(trainX, trainY, epochs=100, batch_size=4, validation_data=validation_data, verbose=2)

        model.save(opj(cfg.base.output_dir,'jw_lstm_model.h5'))
    else:
        from tensorflow.keras.models import load_model
        model = load_model(opj(cfg.base.output_dir,'jw_lstm_model.h5'))
        
        val_pred = model.predict(valX)
        test_pred = model.predict(testX)

        # import pickle
        # with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_21.pkl"), 'wb') as f:
        #     pickle.dump(val_pred.reshape(-1,), f)

        # with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_22.pkl"), 'wb') as f:
        #     pickle.dump(test_pred.reshape(-1,), f)


        # 결과 저장
        pd.DataFrame(data={"date":val_dates_for_infer, cfg.base.task_name:val_pred.reshape(-1,)}).to_csv(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_21.csv"), index=False)

        pd.DataFrame(data={"date":test_dates_for_infer, cfg.base.task_name:test_pred.reshape(-1,)}).to_csv(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_22.csv"), index=False)
       

def create_jw_xgboost(cfg):
    raw_train = pd.read_csv(opj(cfg.base.data_dir, "adj_raw_train.csv"))
    raw_train['date'] = pd.to_datetime(raw_train['date'])

    X, y = raw_train.iloc[:,:-1],raw_train.iloc[:,-1]
    data_dmatrix = XGBRegressor.DMatrix(data=X,label=y)

    target_year=2021
    train = raw_train[raw_train['date'].dt.year<target_year]
    validation = raw_train[raw_train['date'].dt.year==target_year]
    test = raw_train[raw_train['date'].dt.year==(target_year+1)]

    n_train = len(train)
    n_validation = len(validation)
    n_test = len(test)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=n_test, random_state=123, shuffle=False)
    X_train, X_val, y_train, y_val = train_test_split(X_train, y_train, test_size=n_validation, random_state=123, shuffle=False)





    if cfg.base.mode == "train":
        xgb_model = XGBRegressor(n_estimators=500)

        params = {'max_depth':[5,7], 'min_child_weight':[1,3], 'colsample_bytree':[0.5,0.75], 'learning_rate':[0.001,0.05]}
        gridcv = GridSearchCV(xgb_model, param_grid=params, cv=5)


        gridcv.fit(X_train, y_train, early_stopping_rounds=50, eval_metric='rmse',
                    eval_set=[(X_test, y_test), (X_val, y_val)])


        xgb_model = XGBRegressor(n_estimators=100,learning_rate=0.05, max_depth=7, min_child_weight=1, 
                                colsample_bytree=0.75, reg_alpha=0.03)


        xgb_model.fit(X_train, y_train, early_stopping_rounds=50, eval_metric='rmse', eval_set= [(X_train, y_train), 
                                    (X_val, y_val)])


        # 모델 저장
        filename = 'jw_xgboost_model.model'
        joblib.dump(xgb_model, open(opj(cfg.base.output_dir,filename), 'wb'))

    else:
        filename = 'jw_xgboost_model.model'
        with open(opj(cfg.base.output_dir, filename), 'rb') as f:
            xgb_model = joblib.load(f)

        # return xgb_model.predit(validation), xgb_model.predict(test)
        return xgb_model.predict(test)