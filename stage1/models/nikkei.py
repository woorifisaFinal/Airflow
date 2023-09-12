
from os.path import join as opj

__all__ = ["nikkei_xgb", "nikkei_lstm"]
def nikkei_xgb(cfg):
  import pandas as pd
  import numpy as np
  import yfinance as yf
  from pandas_datareader import data as pdr
  import ta
  import joblib

  ##FOR LOCAL
  train = pd.read_csv(opj(cfg.base.data_dir, 'nikkei_train.csv'))
  val = pd.read_csv(opj(cfg.base.data_dir, 'nikkei_val.csv'))
  test = pd.read_csv(opj(cfg.base.data_dir, 'nikkei_test.csv'))
  topix = pd.read_csv(opj(cfg.base.data_dir, 'topix_2017_2021.csv'))
  usd = pd.read_csv(opj(cfg.base.data_dir, 'usd_2017-2021.csv'))
  unemp = pd.read_csv(
    opj(cfg.base.data_dir, 'unemployment_2017_2023.csv'))
  topix_test = pd.read_csv(opj(cfg.base.data_dir, 'topix_2022.csv'))
  usd_test = pd.read_csv(opj(cfg.base.data_dir, 'usd_2022.csv'))
  ##FOR PROJECT
  # train = pd.read_csv(opj(cfg.base.data_dir, 'nikkei_train.csv'))
  # val = pd.read_csv(opj(cfg.base.data_dir, 'nikkei_val.csv'))
  # test = pd.read_csv(opj(cfg.base.data_dir, 'nikkei_test.csv'))
  # topix = pd.read_csv(opj(cfg.base.data_dir, 'topix_2017_2021.csv'))
  # usd = pd.read_csv(opj(cfg.base.data_dir, 'usd_2017-2021.csv)')
  # unemp = pd.read_csv(
  #   opj(cfg.base.data_dir, 'unemployment_2017_2023.csv'))
  # topix_test = pd.read_csv(opj(cfg.base.data_dir, 'topix_2022.csv'))
  # usd_test = pd.read_csv(opj(cfg.base.data_dir, 'usd_2022.csv'))
  #################################################################
  topix['date'] = pd.to_datetime(topix['date'])
  topix = topix.set_index('date')
  usd['date'] = pd.to_datetime(usd['date'])
  usd = usd.set_index('date')
  unemp['date'] = pd.to_datetime(unemp['date'])
  unemp = unemp.set_index('date')  # date/ unemployment

  topix = topix.rename(columns={'close': 'C_topix', 'open': 'O_topix',
                                'high': 'H_topix', 'low': 'L_topix',
                                'change': 'change_topix'})
  usd = usd.rename(columns={'close': 'C_usd', 'open': 'O_usd',
                            'high': 'H_usd', 'low': 'L_usd',
                            'change': 'change_usd'})

  train_end = '2020-12-31'
  val_start = '2021-01-04'
  val_end = '2021-12-31'
  test_start = '2022-01-01'
  test_end = '2022-12-31'

  topix_train = topix[:train_end]
  topix_val = topix[val_start:]
  usd_train = usd[:train_end]
  usd_val = usd[val_start:]

  unemp_train = unemp[:train_end]
  unemp_val = unemp[val_start:val_end]
  unemp_test = unemp[test_start:test_end]

  topix_test['date'] = pd.to_datetime(topix_test['date'])
  topix_test = topix_test.set_index('date')
  topix_test = topix_test.rename(columns={'close': 'C_topix', 'open': 'O_topix',
                                          'high': 'H_topix', 'low': 'L_topix',
                                          'change': 'change_topix'})
  usd_test['date'] = pd.to_datetime(usd_test['date'])
  usd_test = usd_test.set_index('date')
  usd_test = usd_test.rename(columns={'close': 'C_usd', 'open': 'O_usd',
                                      'high': 'H_usd', 'low': 'L_usd',
                                      'change': 'change_usd'})
  H, L, C, V = train['high'], train['low'], train['close'], train['volume']
  train['target'] = train['close'].pct_change()

  train['ATR'] = ta.volatility.average_true_range(high=H, low=L, close=C, fillna=True)
  train['Parabolic SAR'] = ta.trend.psar_down(
    high=H, low=L, close=C, fillna=True)
  train['MACD'] = ta.trend.macd(close=C, fillna=True)
  train['SMA'] = ta.trend.sma_indicator(close=C, fillna=True)
  train['EMA'] = ta.trend.ema_indicator(close=C, fillna=True)
  train['RSI'] = ta.momentum.rsi(close=C, fillna=True)

  train['date'] = pd.to_datetime(train['date'])
  train = train.set_index('date')

  train['day'] = train.index.day
  train['month'] = train.index.month
  train['dayofweek'] = train.index.dayofweek

  train['O_topix'] = topix_train['O_topix']
  train['C_topix'] = topix_train['C_topix']
  train['H_topix'] = topix_train['H_topix']
  train['L_topix'] = topix_train['L_topix']
  train['change_topix'] = topix_train['change_topix']

  train['O_usd'] = usd_train['O_usd']
  train['C_usd'] = usd_train['C_usd']
  train['H_usd'] = usd_train['H_usd']
  train['L_usd'] = usd_train['L_usd']
  train['change_usd'] = usd_train['change_usd']

  train['unemployment'] = unemp_train['unemployment']

  H_val, L_val, C_val, V_val = val['high'], val['low'], val['close'], val['volume']
  val['target'] = val['close'].pct_change()
  val['ATR'] = ta.volatility.average_true_range(high=H_val, low=L_val, close=C_val, fillna=True)
  val['Parabolic SAR'] = ta.trend.psar_down(
    high=H_val, low=L_val, close=C_val, fillna=True)
  val['MACD'] = ta.trend.macd(close=C_val, fillna=True)
  val['SMA'] = ta.trend.sma_indicator(close=C_val, fillna=True)
  val['EMA'] = ta.trend.ema_indicator(close=C_val, fillna=True)
  val['RSI'] = ta.momentum.rsi(close=C_val, fillna=True)

  val['date'] = pd.to_datetime(val['date'])
  val = val.set_index('date')
  val['day'] = val.index.day
  val['month'] = val.index.month
  val['dayofweek'] = val.index.dayofweek

  val['O_topix'] = topix_val['O_topix']
  val['C_topix'] = topix_val['C_topix']
  val['H_topix'] = topix_val['H_topix']
  val['L_topix'] = topix_val['L_topix']
  val['change_topix'] = topix_val['change_topix']

  val['O_usd'] = usd_val['O_usd']
  val['C_usd'] = usd_val['C_usd']
  val['H_usd'] = usd_val['H_usd']
  val['L_usd'] = usd_val['L_usd']
  val['change_usd'] = usd_val['change_usd']

  val['unemployment'] = unemp_val['unemployment']

  H_test, L_test, C_test, V_test = test['high'], test['low'], test['close'], test['volume']
  test['target'] = test['close'].pct_change()

  test['ATR'] = ta.volatility.average_true_range(high=H_test, low=L_test, close=C_test, fillna=True)
  test['Parabolic SAR'] = ta.trend.psar_down(
    high=H_test, low=L_test, close=C_test, fillna=True)
  test['MACD'] = ta.trend.macd(close=C_test, fillna=True)
  test['SMA'] = ta.trend.sma_indicator(close=C_test, fillna=True)
  test['EMA'] = ta.trend.ema_indicator(close=C_test, fillna=True)
  test['RSI'] = ta.momentum.rsi(close=C_test, fillna=True)

  test['date'] = pd.to_datetime(test['date'])
  test = test.set_index('date')
  test['day'] = test.index.day
  test['month'] = test.index.month
  test['dayofweek'] = test.index.dayofweek

  test['O_topix'] = topix_test['O_topix']
  test['C_topix'] = topix_test['C_topix']
  test['H_topix'] = topix_test['H_topix']
  test['L_topix'] = topix_test['L_topix']
  test['change_topix'] = topix_test['change_topix']

  test['O_usd'] = usd_test['O_usd']
  test['C_usd'] = usd_test['C_usd']
  test['H_usd'] = usd_test['H_usd']
  test['L_usd'] = usd_test['L_usd']
  test['change_usd'] = usd_test['change_usd']

  test['unemployment'] = unemp_test['unemployment']

  train = train.fillna(method='bfill')
  val = val.fillna(method='bfill')
  test = test.fillna(method='bfill')
  X_train = pd.DataFrame(train, columns=['month', 'day', 'dayofweek', 'high', 'low',
                                         'volume', 'ATR', 'Parabolic SAR', 'MACD', 'SMA', 'EMA', 'RSI',
                                         'unemployment',
                                         'O_usd', 'C_usd', 'H_usd', 'L_usd', 'change_usd',
                                         'O_topix', 'C_topix', 'H_topix', 'L_topix', 'change_topix',
                                         'close'])
  y_train = train['target']
  X_val = pd.DataFrame(val, columns=['month', 'day', 'dayofweek', 'high', 'low',
                                     'volume', 'ATR', 'Parabolic SAR', 'MACD', 'SMA', 'EMA', 'RSI',
                                     'unemployment',
                                     'O_usd', 'C_usd', 'H_usd', 'L_usd', 'change_usd',
                                     'O_topix', 'C_topix', 'H_topix', 'L_topix', 'change_topix',
                                     'close'])
  y_val = val['target']
  X_test = pd.DataFrame(test, columns=['month', 'day', 'dayofweek', 'high', 'low',
                                       'volume', 'ATR', 'Parabolic SAR', 'MACD', 'SMA', 'EMA', 'RSI',
                                       'unemployment',
                                       'O_usd', 'C_usd', 'H_usd', 'L_usd', 'change_usd',
                                       'O_topix', 'C_topix', 'H_topix', 'L_topix', 'change_topix',
                                       'close'])

  y_test = test['target']
  from xgboost import XGBRegressor
  xgb_model = XGBRegressor(n_estimators=1000, max_depth=5)

  xgb_model.fit(X_train, y_train, eval_metric='rmse', eval_set=[(X_val, y_val)])
  joblib.dump(xgb_model, './models/nikkei_xgb.pkl')


def nikkei_lstm(cfg):

    import pandas as pd
    import numpy as np
    import yfinance as yf
    from pandas_datareader import data as pdr
    import ta
    import joblib

    ##FOR LOCAL
    # train = pd.read_csv(opj(cfg.base.data_dir, 'nikkei_train.csv'))
    # val = pd.read_csv(opj(cfg.base.data_dir, 'nikkei_val.csv'))
    # test = pd.read_csv(opj(cfg.base.data_dir, 'nikkei_test.csv'))
    # topix = pd.read_csv(opj(cfg.base.data_dir, 'topix_2017_2021.csv'))
    # usd = pd.read_csv(opj(cfg.base.data_dir, 'usd_2017-2021.csv)')
    # unemp = pd.read_csv(
    #   opj(cfg.base.data_dir, 'unemployment_2017_2023.csv'))
    # topix_test = pd.read_csv(opj(cfg.base.data_dir, 'topix_2022.csv'))
    # usd_test = pd.read_csv(opj(cfg.base.data_dir, 'usd_2022.csv'))
    ##FOR PROJECT
    train = pd.read_csv(opj(cfg.base.data_dir, 'train_nikkei.csv'))
    val = pd.read_csv(opj(cfg.base.data_dir, 'val_nikkei.csv'))
    test = pd.read_csv(opj(cfg.base.data_dir, 'test_nikkei.csv'))
    # topix = pd.read_csv(opj(cfg.base.data_dir, 'topix_2017_2021.csv'))
    # usd = pd.read_csv(opj(cfg.base.data_dir, 'usd_2017-2021.csv'))
    # unemp = pd.read_csv(
    # opj(cfg.base.data_dir, 'unemployment_2017_2023.csv'))
    # topix_test = pd.read_csv(opj(cfg.base.data_dir, 'topix_2022.csv'))
    # usd_test = pd.read_csv(opj(cfg.base.data_dir, 'usd_2022.csv'))
    #################################################################
    _ = pd.concat([train, val, test])
    dates = pd.to_datetime(_['date'])

    # topix['date'] = pd.to_datetime(topix['date'])
    # topix = topix.set_index('date')
    # usd['date'] = pd.to_datetime(usd['date'])
    # usd = usd.set_index('date')
    # unemp['date'] = pd.to_datetime(unemp['date'])
    # unemp = unemp.set_index('date')  # date/ unemployment

    # topix = topix.rename(columns={'close': 'C_topix', 'open': 'O_topix',
    #                             'high': 'H_topix', 'low': 'L_topix',
    #                             'change': 'change_topix'})
    # usd = usd.rename(columns={'close': 'C_usd', 'open': 'O_usd',
    #                         'high': 'H_usd', 'low': 'L_usd',
    #                         'change': 'change_usd'})

    train_end = '2020-12-31'
    val_start = '2021-01-04'
    val_end = '2021-12-31'
    test_start = '2022-01-01'
    test_end = '2022-12-31'

    # topix_train = topix[:train_end]
    # topix_val = topix[val_start:]
    # usd_train = usd[:train_end]
    # usd_val = usd[val_start:]

    # unemp_train = unemp[:train_end]
    # unemp_val = unemp[val_start:val_end]
    # unemp_test = unemp[test_start:test_end]

    # topix_test['date'] = pd.to_datetime(topix_test['date'])
    # topix_test = topix_test.set_index('date')
    # topix_test = topix_test.rename(columns={'close': 'C_topix', 'open': 'O_topix',
    #                                         'high': 'H_topix', 'low': 'L_topix',
    #                                         'change': 'change_topix'})
    # usd_test['date'] = pd.to_datetime(usd_test['date'])
    # usd_test = usd_test.set_index('date')
    # usd_test = usd_test.rename(columns={'close': 'C_usd', 'open': 'O_usd',
    #                                     'high': 'H_usd', 'low': 'L_usd',
    #                                     'change': 'change_usd'})
    # H, L, C, V = train['high'], train['low'], train['close'], train['volume']
    # train['target'] = train['close'].pct_change()

    # train['ATR'] = ta.volatility.average_true_range(high=H, low=L, close=C, fillna=True)
    # train['Parabolic SAR'] = ta.trend.psar_down(
    # high=H, low=L, close=C, fillna=True)
    # train['MACD'] = ta.trend.macd(close=C, fillna=True)
    # train['SMA'] = ta.trend.sma_indicator(close=C, fillna=True)
    # train['EMA'] = ta.trend.ema_indicator(close=C, fillna=True)
    # train['RSI'] = ta.momentum.rsi(close=C, fillna=True)

    train['date'] = pd.to_datetime(train['date'])
    train = train.set_index('date')

    # train['day'] = train.index.day
    # train['month'] = train.index.month
    # train['dayofweek'] = train.index.dayofweek

    # train['O_topix'] = topix_train['O_topix']
    # train['C_topix'] = topix_train['C_topix']
    # train['H_topix'] = topix_train['H_topix']
    # train['L_topix'] = topix_train['L_topix']
    # train['change_topix'] = topix_train['change_topix']

    # train['O_usd'] = usd_train['O_usd']
    # train['C_usd'] = usd_train['C_usd']
    # train['H_usd'] = usd_train['H_usd']
    # train['L_usd'] = usd_train['L_usd']
    # train['change_usd'] = usd_train['change_usd']

    # train['unemployment'] = unemp_train['unemployment']

    # H_val, L_val, C_val, V_val = val['high'], val['low'], val['close'], val['volume']
    # val['target'] = val['close'].pct_change()
    # val['ATR'] = ta.volatility.average_true_range(high=H_val, low=L_val, close=C_val, fillna=True)
    # val['Parabolic SAR'] = ta.trend.psar_down(
    # high=H_val, low=L_val, close=C_val, fillna=True)
    # val['MACD'] = ta.trend.macd(close=C_val, fillna=True)
    # val['SMA'] = ta.trend.sma_indicator(close=C_val, fillna=True)
    # val['EMA'] = ta.trend.ema_indicator(close=C_val, fillna=True)
    # val['RSI'] = ta.momentum.rsi(close=C_val, fillna=True)

    val['date'] = pd.to_datetime(val['date'])
    val = val.set_index('date')
    # val['day'] = val.index.day
    # val['month'] = val.index.month
    # val['dayofweek'] = val.index.dayofweek

    # val['O_topix'] = topix_val['O_topix']
    # val['C_topix'] = topix_val['C_topix']
    # val['H_topix'] = topix_val['H_topix']
    # val['L_topix'] = topix_val['L_topix']
    # val['change_topix'] = topix_val['change_topix']

    # val['O_usd'] = usd_val['O_usd']
    # val['C_usd'] = usd_val['C_usd']
    # val['H_usd'] = usd_val['H_usd']
    # val['L_usd'] = usd_val['L_usd']
    # val['change_usd'] = usd_val['change_usd']

    # val['unemployment'] = unemp_val['unemployment']

    # H_test, L_test, C_test, V_test = test['high'], test['low'], test['close'], test['volume']
    # test['target'] = test['close'].pct_change()

    # test['ATR'] = ta.volatility.average_true_range(high=H_test, low=L_test, close=C_test, fillna=True)
    # test['Parabolic SAR'] = ta.trend.psar_down(
    # high=H_test, low=L_test, close=C_test, fillna=True)
    # test['MACD'] = ta.trend.macd(close=C_test, fillna=True)
    # test['SMA'] = ta.trend.sma_indicator(close=C_test, fillna=True)
    # test['EMA'] = ta.trend.ema_indicator(close=C_test, fillna=True)
    # test['RSI'] = ta.momentum.rsi(close=C_test, fillna=True)

    test['date'] = pd.to_datetime(test['date'])
    test = test.set_index('date')
    # test['day'] = test.index.day
    # test['month'] = test.index.month
    # test['dayofweek'] = test.index.dayofweek

    # test['O_topix'] = topix_test['O_topix']
    # test['C_topix'] = topix_test['C_topix']
    # test['H_topix'] = topix_test['H_topix']
    # test['L_topix'] = topix_test['L_topix']
    # test['change_topix'] = topix_test['change_topix']

    # test['O_usd'] = usd_test['O_usd']
    # test['C_usd'] = usd_test['C_usd']
    # test['H_usd'] = usd_test['H_usd']
    # test['L_usd'] = usd_test['L_usd']
    # test['change_usd'] = usd_test['change_usd']

    # test['unemployment'] = unemp_test['unemployment']

    train = train.fillna(method='bfill')
    val = val.fillna(method='bfill')
    test = test.fillna(method='bfill')
    X_train = pd.DataFrame(train, columns=['month', 'day', 'dayofweek', 'high', 'low',
                                            'volume', 'ATR', 'Parabolic SAR', 'MACD', 'SMA', 'EMA', 'RSI',
                                            'unemployment',
                                            'O_usd', 'C_usd', 'H_usd', 'L_usd', 'change_usd',
                                            'O_topix', 'C_topix', 'H_topix', 'L_topix', 'change_topix',
                                            'close'])
    y_train = train['target']
    X_val = pd.DataFrame(val, columns=['month', 'day', 'dayofweek', 'high', 'low',
                                        'volume', 'ATR', 'Parabolic SAR', 'MACD', 'SMA', 'EMA', 'RSI',
                                        'unemployment',
                                        'O_usd', 'C_usd', 'H_usd', 'L_usd', 'change_usd',
                                        'O_topix', 'C_topix', 'H_topix', 'L_topix', 'change_topix',
                                        'close'])
    y_val = val['target']
    X_test = pd.DataFrame(test, columns=['month', 'day', 'dayofweek', 'high', 'low',
                                        'volume', 'ATR', 'Parabolic SAR', 'MACD', 'SMA', 'EMA', 'RSI',
                                        'unemployment',
                                        'O_usd', 'C_usd', 'H_usd', 'L_usd', 'change_usd',
                                        'O_topix', 'C_topix', 'H_topix', 'L_topix', 'change_topix',
                                        'close'])

    y_test = test['target']

    train = train.fillna(train.mean())
    val = val.fillna(val.mean())
    test = test.fillna(test.mean())

    from sklearn.preprocessing import MinMaxScaler, StandardScaler
    
    if cfg.base.mode=='train':
        scaler = StandardScaler()
        df = pd.concat([train, val])
        scaled_df = scaler.fit_transform(df)
        scaled_test = scaler.transform(test)
        joblib.dump(scaler, opj(cfg.base.output_dir, 'nikkei_scaler.pkl'))
    else:
        scaler = joblib.load(opj(cfg.base.output_dir,'nikkei_scaler.pkl'))
        df = pd.concat([train, val])#.drop("date", axis=1)

        # fit_transform을 transform으로 변경
        scaled_df = scaler.transform(df)
        scaled_test = scaler.transform(test)


    n_train = len(train)
    n_val = len(train) + len(val)
    n_test = n_val

    train_data_scaled = scaled_df[0: n_train]
    train_dates = dates[0: n_train]

    val_data_scaled = scaled_df[n_train: n_val]
    val_dates = dates[n_train: n_val]

    test_data_scaled = scaled_test[:]
    test_dates = dates[n_test:]

    import numpy as np
    # data reformatting for LSTM
    pred_days = 30  # prediction period - 3months
    seq_len = 50  # sequence length = past days for future prediction.
    input_dim =  train.shape[1]  # input_dimension = ['close', 'open', 'high', 'low', 'rsi', 'MACD_12_26', 'MACD_sign_12_26', 'hband', 'mavg', 'lband', 'CSI', 'target']

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
    for i in range(seq_len, n_train - pred_days + 1):
        trainX.append(train_data_scaled[i - seq_len:i, 0:train_data_scaled.shape[1]])
        trainY.append(train_data_scaled[i + pred_days - 1:i + pred_days, 0])

    for i in range(seq_len, len(val_data_scaled) - pred_days + 1):
        valX.append(val_data_scaled[i - seq_len:i, 0:val_data_scaled.shape[1]])
        valY.append(val_data_scaled[i + pred_days - 1:i + pred_days, 0])
        # base_dates추가
        val_dates_for_infer.append(val_dates[i + pred_days - 1:i + pred_days].values[0])

    for i in range(seq_len, len(test_data_scaled) - pred_days + 1):
        # print("testX :", testX)
        # print("testX.shape :", len(testX))
        testX.append(test_data_scaled[i - seq_len:i, 0:test_data_scaled.shape[1]])
        testY.append(test_data_scaled[i + pred_days - 1:i + pred_days, 0])
        # base_dates추가
        test_dates_for_infer.append(test_dates[i + pred_days - 1:i + pred_days].values[0])

    trainX, trainY = np.array(trainX), np.array(trainY)
    valX, valY = np.array(valX), np.array(valY)
    testX, testY = np.array(testX), np.array(testY)

    from tensorflow.keras import Sequential
    from tensorflow.keras.layers import Dense, LSTM, Dropout
    from tensorflow.keras.optimizers import Adam
    import tensorflow as tf
    if cfg.base.mode =='train':
        model = Sequential()
        model.add(LSTM(units=20, return_sequences=True, input_shape=(trainX.shape[1], trainX.shape[2])))

        model.add(LSTM(units=20))

        model.add(Dense(units=1))
        model.compile(optimizer="Adam", loss='mse', metrics=['mae'])
        model.fit(trainX, trainY, epochs=30, batch_size=15, validation_data=(valX, valY))
        # joblib.dump(model, './models/nikkei_lstm.pkl')
        model.save(opj(cfg.base.output_dir,"nikkei_lstm.h5"))

    else:
       
        # model = Sequential()
        # model.add(LSTM(units=20, return_sequences=True, input_shape=(trainX.shape[1], trainX.shape[2])))
        # model.add(Dropout(0.1))
        # model.add(LSTM(units=20))
        # model.add(Dropout(0.1))
        # model.add(Dense(units=1))
        # model.compile(optimizer="Adam", loss='mse', metrics=['mae'])
        # model.fit(trainX, trainY, epochs=30, batch_size=20, validation_data=(valX, valY))
        # joblib.dump(model, opj(cfg.base.output_dir,'nasdaq_lstm.pkl'))
        

        # model = joblib.load(opj(cfg.base.output_dir,'nasdaq_lstm.pkl'))
        from keras.models import load_model
        model = load_model(opj(cfg.base.output_dir,"nikkei_lstm.h5"))
        val_pred = model.predict(valX)
        test_pred = model.predict(testX)

        # val_dates.to_csv(opj(cfg.base.output_dir, "hi.csv"))
        # import pickle
        # with open(opj(cfg.base.output_dir, "predicted.pkl"), 'wb') as f:
        #     pickle.dump(val_pred, f)
        # val_dates['predict'] = val_pred


        # val_dates[['predict']].to_csv(f"{cfg.base.user_name}_{2021}")
        # val_dates[['predict']].to_csv(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_21.csv"))

        # test_dates['predict'] = test_pred

        # test_dates[['predict']].to_csv(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_22.csv"))

        # return val_pred, test_pred
        
        import pickle
        # with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_21.pkl"), 'wb') as f:
        #     pickle.dump(val_pred.reshape(-1,), f)

        # with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_22.pkl"), 'wb') as f:
        #     pickle.dump(test_pred.reshape(-1,), f)

        ############### for debugging #################
        # with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}__aa.pkl"), 'wb') as f:
        #     pickle.dump(test_pred.reshape(-1,), f)
        # with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_date.pkl"), 'wb') as f:
        #     pickle.dump(test_dates_for_infer, f)
        # with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_test_date.pkl"), 'wb') as f:
        #     pickle.dump(test_dates, f)

        # 결과 저장
        pd.DataFrame(data={"date":val_dates_for_infer, "jp":val_pred.reshape(-1,)}).to_csv(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_21.csv"), index=False)
        pd.DataFrame(data={"date":test_dates_for_infer, "jp":test_pred.reshape(-1,)}).to_csv(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_22.csv"), index=False)
       