from sklearn.metrics import confusion_matrix, classification_report, roc_curve, roc_auc_score
from sklearn.metrics import precision_recall_curve, auc, precision_score, recall_score, f1_score
from xgboost import plot_importance
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import xgboost as xgb
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import MinMaxScaler
from xgboost import XGBRegressor
from sklearn.model_selection import RandomizedSearchCV

def bond_short():
  data = pd.read_csv('/content/3년국채 데이터17_22.csv', encoding='cp949')
  data['날짜'] = pd.to_datetime(data['날짜'])
  data = data.sort_values(by='날짜')
  data = data[['날짜', '종가', '시가', '저가', '수익률', '변동률']]
  data['종가'] = data['종가'].str.replace(',', '').astype(float)
  data['시가'] = data['시가'].str.replace(',', '').astype(float)
  data['저가'] = data['저가'].str.replace(',', '').astype(float)
  data['수익률'] = data['수익률'].str.replace('%', '').astype(float) / 100.0
  data['변동률'] = data['변동률'].str.replace('%', '').astype(float) / 100.0

  # 17년부터 20년까지의 데이터를 train, 21년을 validation, 22년을 test 데이터로 사용
  train_data = data[(data['날짜'].dt.year >= 2017) & (data['날짜'].dt.year <= 2020)]
  validation_data = data[data['날짜'].dt.year == 2021]
  test_data = data[data['날짜'].dt.year == 2022]


  # 각각 저장
  train_data.data = data[data['날짜'].dt.year <= 2020]
  validation_data = data[data['날짜'].dt.year == 2021]
  test_data = data[data['날짜'].dt.year == 2022]

  train_data.to_csv("kr_short_train.csv", encoding="euc-kr")
  validation_data.to_csv("kr_short_validation.csv", encoding="euc-kr")
  test_data.to_csv("kr_short_test.csv", encoding="euc-kr")


  # Train 데이터 전처리
  X_train = train_data.drop(columns=['수익률','날짜'])
  y_train = train_data['수익률']

  # Validation 데이터 전처리
  X_validation = validation_data.drop(columns=['수익률','날짜'])
  y_validation = validation_data['수익률']

  # Test 데이터 전처리
  X_test = test_data.drop(columns=['수익률','날짜'])
  y_test = test_data['수익률']

  validation_data_ = (X_validation,y_validation)



  # XGBoost 모델 생성 (랜덤 탐색용으로 사용)
  # xgb_model = XGBRegressor()

  # 랜덤 탐색 수행
  # random_search = RandomizedSearchCV(
  # xgb_model, param_distributions=param_dist, n_iter=200,  # n_iter를 늘림
  # scoring='neg_mean_squared_error', cv=3, verbose=2, random_state=42, n_jobs=-1
  # )

  # # 랜덤 탐색 실행
  # random_search.fit(X_train, y_train)

  # # 최적의 하이퍼파라미터 출력
  # print("Best Hyperparameters:", random_search.best_params_)

  # # 하이퍼파라미터 탐색 대상
  # param_dist = {
  # 'n_estimators': [100],#
  # 'learning_rate': [0.45],#
  # 'max_depth': [7],#
  # 'min_child_weight': [6],#
  # 'subsample': [1],#
  # 'colsample_bytree': [0.7],#
  # 'gamma': [0]#

  # }


  # # 최적의 모델로 예측 수행 (Validation 데이터에 대해서)
  # best_model = random_search.best_estimator_
  # predictions_validation = best_model.predict(X_validation)

  # XGBoost 모델 생성
  xgb_model = XGBRegressor(n_estimators=100, learning_rate=0.45, max_depth=7,
  min_child_weight=6, subsample=1, colsample_bytree=0.7,
  gamma=0, validation_data=validation_data_)

  # 모델 학습
  xgb_model.fit(X_train, y_train)


from sklearn.metrics import confusion_matrix, classification_report, roc_curve, roc_auc_score
from sklearn.metrics import precision_recall_curve, auc, precision_score, recall_score, f1_score
from xgboost import plot_importance
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import xgboost as xgb
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import MinMaxScaler
from xgboost import XGBRegressor
from sklearn.model_selection import RandomizedSearchCV

def bond_long():
    data = pd.read_csv('/content/한국10 병합.csv', encoding='cp949')
    data['날짜'] = pd.to_datetime(data['날짜'])
    data = data.sort_values(by='날짜')
    data = data[['날짜', '종가', '시가', '저가', '수익률', '변동률']]
    # data['종가'] = data['종가'].str.replace(',', '').astype(float)
    # data['시가'] = data['시가'].str.replace(',', '').astype(float)
    # data['저가'] = data['저가'].str.replace(',', '').astype(float)
    # data['수익률'] = data['수익률'].str.replace('%', '').astype(float) / 100.0
    # data['변동률'] = data['변동률'].str.replace('%', '').astype(float) / 100.0

    # 17년부터 20년까지의 데이터를 train, 21년을 validation, 22년을 test 데이터로 사용
    train_data = data[(data['날짜'].dt.year >= 2017) & (data['날짜'].dt.year <= 2020)]
    validation_data = data[data['날짜'].dt.year == 2021]
    test_data = data[data['날짜'].dt.year == 2022]

    train_data.to_csv("kr_long_train.csv", encoding="euc-kr")
    validation_data.to_csv("kr_long_validation.csv", encoding="euc-kr")
    test_data.to_csv("kr_long_test.csv", encoding="euc-kr")


    # Train 데이터 전처리
    X_train = train_data.drop(columns=['수익률','날짜'])
    y_train = train_data['수익률']

    # Validation 데이터 전처리
    X_validation = validation_data.drop(columns=['수익률','날짜'])
    y_validation = validation_data['수익률']

    # Test 데이터 전처리
    X_test = test_data.drop(columns=['수익률','날짜'])
    y_test = test_data['수익률']

    validation_data_ = (X_validation,y_validation)


    from sklearn.metrics import confusion_matrix, classification_report, roc_curve, roc_auc_score
from sklearn.metrics import precision_recall_curve, auc, precision_score, recall_score, f1_score
from xgboost import plot_importance
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import xgboost as xgb
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import MinMaxScaler
from xgboost import XGBRegressor
from sklearn.model_selection import RandomizedSearchCV

def us_bond_short():
    data = pd.read_csv('/content/미국3 병합.csv', encoding='cp949')
    data['날짜'] = pd.to_datetime(data['날짜'])
    data = data.sort_values(by='날짜')
    data = data[['날짜', '종가', '시가', '저가', '수익률', '변동률']]
    # data['종가'] = data['종가'].str.replace(',', '').astype(float)
    # data['시가'] = data['시가'].str.replace(',', '').astype(float)
    # data['저가'] = data['저가'].str.replace(',', '').astype(float)
    # data['수익률'] = data['수익률'].str.replace('%', '').astype(float) / 100.0
    # data['변동률'] = data['변동률'].str.replace('%', '').astype(float) / 100.0

    # 17년부터 20년까지의 데이터를 train, 21년을 validation, 22년을 test 데이터로 사용
    train_data = data[(data['날짜'].dt.year >= 2017) & (data['날짜'].dt.year <= 2020)]
    validation_data = data[data['날짜'].dt.year == 2021]
    test_data = data[data['날짜'].dt.year == 2022]



    train_data.to_csv("us_short_train.csv", encoding="euc-kr")
    validation_data.to_csv("us_short_validation.csv", encoding="euc-kr")
    test_data.to_csv("us_short_test.csv", encoding="euc-kr")



    # Train 데이터 전처리
    X_train = train_data.drop(columns=['수익률','날짜'])
    y_train = train_data['수익률']

    # Validation 데이터 전처리
    X_validation = validation_data.drop(columns=['수익률','날짜'])
    y_validation = validation_data['수익률']

    # Test 데이터 전처리
    X_test = test_data.drop(columns=['수익률','날짜'])
    y_test = test_data['수익률']

    validation_data_ = (X_validation,y_validation)



    # XGBoost 모델 생성 (랜덤 탐색용으로 사용)
    # xgb_model = XGBRegressor()

    # 랜덤 탐색 수행
    # random_search = RandomizedSearchCV(
    #     xgb_model, param_distributions=param_dist, n_iter=200,  # n_iter를 늘림
    #     scoring='neg_mean_squared_error', cv=3, verbose=2, random_state=42, n_jobs=-1
    # )

    # # 랜덤 탐색 실행
    # random_search.fit(X_train, y_train)

    # # 최적의 하이퍼파라미터 출력
    # print("Best Hyperparameters:", random_search.best_params_)

    # # 하이퍼파라미터 탐색 대상
    # param_dist = {
    #     'n_estimators': [100],#
    #     'learning_rate': [0.45],#
    #     'max_depth': [7],#
    #     'min_child_weight': [6],#
    #     'subsample': [1],#
    #     'colsample_bytree': [0.7],#
    #     'gamma': [0]#

    # }


    # # 최적의 모델로 예측 수행 (Validation 데이터에 대해서)
    # best_model = random_search.best_estimator_
    # predictions_validation = best_model.predict(X_validation)

    # XGBoost 모델 생성
    xgb_model = XGBRegressor(n_estimators=100, learning_rate=0.45, max_depth=7,
                            min_child_weight=6, subsample=1, colsample_bytree=0.7,
                            gamma=0, validation_data=validation_data_)

    # 모델 학습
    xgb_model.fit(X_train, y_train)


from sklearn.metrics import confusion_matrix, classification_report, roc_curve, roc_auc_score
from sklearn.metrics import precision_recall_curve, auc, precision_score, recall_score, f1_score
from xgboost import plot_importance
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import xgboost as xgb
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import MinMaxScaler
from xgboost import XGBRegressor
from sklearn.model_selection import RandomizedSearchCV

def us_bond_short():
    data = pd.read_csv('/content/미국10 병합.csv', encoding='cp949')
    data['날짜'] = pd.to_datetime(data['날짜'])
    data = data.sort_values(by='날짜')
    data = data[['날짜', '종가', '시가', '저가', '수익률', '변동률']]
    # data['종가'] = data['종가'].str.replace(',', '').astype(float)
    # data['시가'] = data['시가'].str.replace(',', '').astype(float)
    # data['저가'] = data['저가'].str.replace(',', '').astype(float)
    # data['수익률'] = data['수익률'].str.replace('%', '').astype(float) / 100.0
    # data['변동률'] = data['변동률'].str.replace('%', '').astype(float) / 100.0

    # 17년부터 20년까지의 데이터를 train, 21년을 validation, 22년을 test 데이터로 사용
    train_data = data[(data['날짜'].dt.year >= 2017) & (data['날짜'].dt.year <= 2020)]
    validation_data = data[data['날짜'].dt.year == 2021]
    test_data = data[data['날짜'].dt.year == 2022]



    train_data.to_csv("us_long_train.csv", encoding="euc-kr")
    validation_data.to_csv("us_long_validation.csv", encoding="euc-kr")
    test_data.to_csv("us_long_test.csv", encoding="euc-kr")



    # Train 데이터 전처리
    X_train = train_data.drop(columns=['수익률','날짜'])
    y_train = train_data['수익률']

    # Validation 데이터 전처리
    X_validation = validation_data.drop(columns=['수익률','날짜'])
    y_validation = validation_data['수익률']

    # Test 데이터 전처리
    X_test = test_data.drop(columns=['수익률','날짜'])
    y_test = test_data['수익률']

    validation_data_ = (X_validation,y_validation)



    # XGBoost 모델 생성 (랜덤 탐색용으로 사용)
    # xgb_model = XGBRegressor()

    # 랜덤 탐색 수행
    # random_search = RandomizedSearchCV(
    #     xgb_model, param_distributions=param_dist, n_iter=200,  # n_iter를 늘림
    #     scoring='neg_mean_squared_error', cv=3, verbose=2, random_state=42, n_jobs=-1
    # )

    # # 랜덤 탐색 실행
    # random_search.fit(X_train, y_train)

    # # 최적의 하이퍼파라미터 출력
    # print("Best Hyperparameters:", random_search.best_params_)

    # # 하이퍼파라미터 탐색 대상
    # param_dist = {
    #     'n_estimators': [100],#
    #     'learning_rate': [0.45],#
    #     'max_depth': [7],#
    #     'min_child_weight': [6],#
    #     'subsample': [1],#
    #     'colsample_bytree': [0.7],#
    #     'gamma': [0]#

    # }


    # # 최적의 모델로 예측 수행 (Validation 데이터에 대해서)
    # best_model = random_search.best_estimator_
    # predictions_validation = best_model.predict(X_validation)

    # XGBoost 모델 생성
    xgb_model = XGBRegressor(n_estimators=100, learning_rate=0.45, max_depth=7,
                            min_child_weight=6, subsample=1, colsample_bytree=0.7,
                            gamma=0, validation_data=validation_data_)

    # 모델 학습
    xgb_model.fit(X_train, y_train)
