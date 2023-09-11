
from os.path import join as opj

__all__ = ["nasdaq_xgb", "nasdaq_lstm"]
def nasdaq_xgb(cfg):
    if cfg.mode=="train":
        import pandas as pd
        import numpy as np
        import yfinance as yf
        from pandas_datareader import data as pdr
        import ta
        import joblib

        # FOR LOCAL
        # train = pd.read_csv(opj(cfg.base.data_dir, 'train_nasdaq.csv'))
        # val = pd.read_csv(opj(cfg.base.data_dir, 'val_nasdaq.csv'))
        # test = pd.read_csv(opj(cfg.base.data_dir, 'test_nasdaq.csv'))
        # tips = pd.read_csv(opj(cfg.base.data_dir, 'tips_2017_2021.csv'))
        # cpi = pd.read_csv(opj(cfg.base.data_dir, 'cpi_2017_2021.csv'))
        # vix = pd.read_csv(opj(cfg.base.data_dir, 'vix_2017_2021.csv'))
        # sp = pd.read_csv(opj(cfg.base.data_dir, 'sp500.csv'))
        # rut = pd.read_csv(opj(cfg.base.data_dir, 'russel.csv'))
        # cpi_test = pd.read_csv(opj(cfg.base.data_dir, 'cpi_2022.csv'))
        # tips_test = pd.read_csv(opj(cfg.base.data_dir, 'tips_2022.csv'))
        # vix_test = pd.read_csv(opj(cfg.base.data_dir, 'vix_2022.csv'))

        ## FOR PROJECT
        train = pd.read_csv(opj(cfg.base.data_dir, 'train_nasdaq.csv'))
        val = pd.read_csv(opj(cfg.base.data_dir, 'val_nasdaq.csv'))
        test = pd.read_csv(opj(cfg.base.data_dir, 'test_nasdaq.csv'))
        tips = pd.read_csv(opj(cfg.base.data_dir, 'tips_2017_2021.csv'))
        cpi = pd.read_csv(opj(cfg.base.data_dir, 'cpi_2017_2021.csv'))
        vix = pd.read_csv(opj(cfg.base.data_dir, 'vix_2017_2021.csv'))
        sp = pd.read_csv(opj(cfg.base.data_dir, 'sp500.csv'))
        rut = pd.read_csv(opj(cfg.base.data_dir, 'russel.csv'))
        cpi_test = pd.read_csv(opj(cfg.base.data_dir, 'cpi_2022.csv'))
        tips_test = pd.read_csv(opj(cfg.base.data_dir, 'tips_2022.csv'))
        vix_test = pd.read_csv(opj(cfg.base.data_dir, 'vix_2022.csv'))

        # tips, cpi, vix는 test csv가 따로 저장되어있고 sp,rut은 23년 최근까지 다 받아온 csv



        tips['date'] = pd.to_datetime(tips['date'])
        tips = tips.set_index('date')
        cpi['date'] = pd.to_datetime(cpi['date'])
        cpi = cpi.set_index('date')
        vix['date'] = pd.to_datetime(vix['date'])
        vix = vix.set_index('date')
        sp['date'] = pd.to_datetime(sp['date'])
        sp = sp.set_index('date')
        rut['date'] = pd.to_datetime(rut['date'])
        rut = rut.set_index('date')

        tips = tips.rename(columns={'close': 'tips'})
        vix = vix.rename(
          columns={'close': 'C_vix', 'open': 'O_vix', 'high': 'H_vix', 'low': 'L_vix', 'volatility': 'change_vix'})

        train_end = '2020-12-31'
        val_start = '2021-01-04'
        val_end = '2021-12-31'
        test_start = '2022-01-01'
        test_end = '2022-12-31'

        tips_train = tips[:train_end]
        tips_val = tips[val_start:]
        vix_train = vix[:train_end]
        vix_val = vix[val_start:]

        cpi_train = cpi[:train_end]
        cpi_val = cpi[val_start:]

        sp_train = sp[:train_end]
        sp_val = sp[val_start:val_end]
        sp_test = sp[test_start : test_end]

        rut_train = rut[:train_end]
        rut_val = rut[val_start:val_end]
        rut_test = rut[test_start:test_end]


        cpi_test['date'] = pd.to_datetime(cpi_test['date'])
        cpi_test = cpi_test.set_index('date')


        tips_test['date'] = pd.to_datetime(tips_test['date'])
        tips_test = tips_test.set_index('date')
        tips_test = tips_test.rename(columns={'close_tips': 'tips'})

        vix_test['date'] = pd.to_datetime(vix_test['date'])
        vix_test = vix_test.set_index('date')
        ############################################################################
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

        train['tips'] = tips_train['tips']
        train['real_cpi'] = cpi_train['real_cpi']
        train['pred_cpi'] = cpi_train['pred_cpi']
        train['past_cpi'] = cpi_train['past_cpi']
        train['O_vix'] = vix_train['O_vix']
        train['C_vix'] = vix_train['C_vix']
        train['H_vix'] = vix_train['H_vix']
        train['L_vix'] = vix_train['L_vix']
        train['change_vix'] = vix_train['change_vix']
        ############################################################################

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

        val['tips'] = tips_val['tips']
        val['real_cpi'] = cpi_val['real_cpi']
        val['pred_cpi'] = cpi_val['pred_cpi']
        val['past_cpi'] = cpi_val['past_cpi']
        val['O_vix'] = vix_val['O_vix']
        val['C_vix'] = vix_val['C_vix']
        val['H_vix'] = vix_val['H_vix']
        val['L_vix'] = vix_val['L_vix']
        val['change_vix'] = vix_val['change_vix']

        ############################################################################
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

        test['tips'] = tips_test['tips']
        test['real_cpi'] = cpi_test['real_cpi']
        test['pred_cpi'] = cpi_test['pred_cpi']
        test['past_cpi'] = cpi_test['past_cpi']
        test['O_vix'] = vix_test['open_vix']
        test['C_vix'] = vix_test['close_vix']
        test['H_vix'] = vix_test['high_vix']
        test['L_vix'] = vix_test['low_vix']
        test['change_vix'] = vix_test['change_vix']

        ###################################################################
        train = train.fillna(method='bfill')
        val = val.fillna(method='bfill')
        test = test.fillna(method='bfill')

        ###################################################################
        X_train = pd.DataFrame(train, columns=['month', 'day', 'dayofweek', 'high', 'low',
                                                'volume', 'ATR', 'Parabolic SAR', 'MACD', 'SMA', 'EMA', 'RSI',
                                                'tips', 'real_cpi', 'pred_cpi', 'past_cpi',
                                                'O_vix', 'C_vix', 'H_vix', 'L_vix', 'change_vix', 'close'])
        y_train = train['target']
        ####################################################################
        X_val = pd.DataFrame(val, columns=['month', 'day', 'dayofweek', 'high', 'low',
                                            'volume', 'ATR', 'Parabolic SAR', 'MACD', 'SMA', 'EMA', 'RSI',
                                            'tips', 'real_cpi', 'pred_cpi', 'past_cpi',
                                            'O_vix', 'C_vix', 'H_vix', 'L_vix', 'change_vix', 'close'])
        y_val = val['target']
        ####################################################################
        test_dates = pd.DataFrame(test, columns=['month', 'day', 'dayofweek', 'high', 'low',
                                              'volume', 'ATR', 'Parabolic SAR', 'MACD', 'SMA', 'EMA', 'RSI',
                                              'tips', 'real_cpi', 'pred_cpi', 'past_cpi',
                                              'O_vix', 'C_vix', 'H_vix', 'L_vix', 'change_vix', 'close'])

        y_test = test['target']
        ####################################################################
        from xgboost import XGBRegressor


        xgb_model = XGBRegressor(n_estimators=1000, max_depth=5)

        xgb_model.fit(X_train, y_train, eval_metric='rmse', eval_set=[(X_val, y_val)])

        joblib.dump(xgb_model, opj(cfg.base.output_dir,'/nasdaq_xgb.pkl'))

    
    else:
        from xgboost import XGBRegressor
        import joblib

        import pandas as pd
        import numpy as np
        import yfinance as yf
        from pandas_datareader import data as pdr
        import ta
        import joblib
        
        val = pd.read_csv(opj(cfg.base.data_dir, 'val_nasdaq.csv'))
        test = pd.read_csv(opj(cfg.base.data_dir, 'test_nasdaq.csv'))

        val = val.fillna(method='bfill')
        test = test.fillna(method='bfill')

        ####################################################################
        X_val = pd.DataFrame(val, columns=['month', 'day', 'dayofweek', 'high', 'low',
                                            'volume', 'ATR', 'Parabolic SAR', 'MACD', 'SMA', 'EMA', 'RSI',
                                            'tips', 'real_cpi', 'pred_cpi', 'past_cpi',
                                            'O_vix', 'C_vix', 'H_vix', 'L_vix', 'change_vix', 'close'])
        y_val = val['target']
        ####################################################################
        X_test = pd.DataFrame(test, columns=['month', 'day', 'dayofweek', 'high', 'low',
                                              'volume', 'ATR', 'Parabolic SAR', 'MACD', 'SMA', 'EMA', 'RSI',
                                              'tips', 'real_cpi', 'pred_cpi', 'past_cpi',
                                              'O_vix', 'C_vix', 'H_vix', 'L_vix', 'change_vix', 'close'])

        y_test = test['target']
        ####################################################################


        xgb_model = XGBRegressor(n_estimators=1000, max_depth=5)

        xgb_model = joblib.load(opj(cfg.base.output_dir,'/nasdaq_xgb.pkl'))

        val_pred = xgb_model.predict(X_val)
        test_pred = xgb_model.predict(X_test)

        X_val['predict'] = val_pred
        
        # X_val[['predict']].to_csv(f"{cfg.base.user_name}_{2021}")
        X_val[['predict']].to_csv(f"{cfg.base.task_name}_prediction_21.csv")

        X_test['predict'] = test_pred

        X_test[['predict']].to_csv(f"{cfg.base.task_name}_prediction_22.csv")

        # return val_pred, test_pred
def nasdaq_lstm(cfg):
    import pandas as pd
    import numpy as np
    import yfinance as yf
    from pandas_datareader import data as pdr
    import ta
    import joblib

    ## FOR LOCAL
    # train = pd.read_csv(opj(cfg.base.data_dir, 'train_nasdaq.csv'))
    # val = pd.read_csv(opj(cfg.base.data_dir, 'val_nasdaq.csv'))
    # test = pd.read_csv(opj(cfg.base.data_dir, 'test_nasdaq.csv'))
    # tips = pd.read_csv(opj(cfg.base.data_dir, 'tips_2017_2021.csv'))
    # cpi = pd.read_csv(opj(cfg.base.data_dir, 'cpi_2017_2021.csv'))
    # vix = pd.read_csv(opj(cfg.base.data_dir, 'vix_2017_2021.csv'))
    # sp = pd.read_csv(opj(cfg.base.data_dir, 'sp500.csv'))
    # rut = pd.read_csv(opj(cfg.base.data_dir, 'russel.csv'))
    # cpi_test = pd.read_csv(opj(cfg.base.data_dir, 'cpi_2022.csv'))
    # tips_test = pd.read_csv(opj(cfg.base.data_dir, 'tips_2022.csv'))
    # vix_test = pd.read_csv(opj(cfg.base.data_dir, 'vix_2022.csv'))

    ## FOR PROJECT
    train = pd.read_csv(opj(cfg.base.data_dir, 'train_nasdaq.csv'))
    val = pd.read_csv(opj(cfg.base.data_dir, 'val_nasdaq.csv'))
    test = pd.read_csv(opj(cfg.base.data_dir, 'test_nasdaq.csv'))
    # tips = pd.read_csv(opj(cfg.base.data_dir, 'tips_2017_2021.csv'))
    # cpi = pd.read_csv(opj(cfg.base.data_dir, 'cpi_2017_2021.csv'))
    # vix = pd.read_csv(opj(cfg.base.data_dir, 'vix_2017_2021.csv'))
    # sp = pd.read_csv(opj(cfg.base.data_dir, 'sp500.csv'))
    # rut = pd.read_csv(opj(cfg.base.data_dir, 'russel.csv'))
    # cpi_test = pd.read_csv(opj(cfg.base.data_dir, 'cpi_2022.csv'))
    # tips_test = pd.read_csv(opj(cfg.base.data_dir, 'tips_2022.csv'))
    # vix_test = pd.read_csv(opj(cfg.base.data_dir, 'vix_2022.csv'))

    # tips, cpi, vix는 test csv가 따로 저장되어있고 sp,rut은 23년 최근까지 다 받아온 csv

    _ = pd.concat([train, val, test])
    dates = pd.to_datetime(_['date'])

    # tips['date'] = pd.to_datetime(tips['date'])
    # tips = tips.set_index('date')
    # cpi['date'] = pd.to_datetime(cpi['date'])
    # cpi = cpi.set_index('date')
    # vix['date'] = pd.to_datetime(vix['date'])
    # vix = vix.set_index('date')
    # sp['date'] = pd.to_datetime(sp['date'])
    # sp = sp.set_index('date')
    # rut['date'] = pd.to_datetime(rut['date'])
    # rut = rut.set_index('date')

    # tips = tips.rename(columns={'close': 'tips'})
    # vix = vix.rename(
    # columns={'close': 'C_vix', 'open': 'O_vix', 'high': 'H_vix', 'low': 'L_vix', 'volatility': 'change_vix'})

    train_end = '2020-12-31'
    val_start = '2021-01-04'
    val_end = '2021-12-31'
    test_start = '2022-01-01'
    test_end = '2022-12-31'

    # tips_train = tips[:train_end]
    # tips_val = tips[val_start:]
    # vix_train = vix[:train_end]
    # vix_val = vix[val_start:]

    # cpi_train = cpi[:train_end]
    # cpi_val = cpi[val_start:]

    # sp_train = sp[:train_end]
    # sp_val = sp[val_start:val_end]
    # sp_test = sp[test_start: test_end]

    # rut_train = rut[:train_end]
    # rut_val = rut[val_start:val_end]
    # rut_test = rut[test_start:test_end]

    # cpi_test['date'] = pd.to_datetime(cpi_test['date'])
    # cpi_test = cpi_test.set_index('date')

    # tips_test['date'] = pd.to_datetime(tips_test['date'])
    # tips_test = tips_test.set_index('date')
    # tips_test = tips_test.rename(columns={'close_tips': 'tips'})

    # vix_test['date'] = pd.to_datetime(vix_test['date'])
    # vix_test = vix_test.set_index('date')
    # ############################################################################
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

    # train['tips'] = tips_train['tips']
    # train['real_cpi'] = cpi_train['real_cpi']
    # train['pred_cpi'] = cpi_train['pred_cpi']
    # train['past_cpi'] = cpi_train['past_cpi']
    # train['O_vix'] = vix_train['O_vix']
    # train['C_vix'] = vix_train['C_vix']
    # train['H_vix'] = vix_train['H_vix']
    # train['L_vix'] = vix_train['L_vix']
    # train['change_vix'] = vix_train['change_vix']
    # ############################################################################

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

    # val['tips'] = tips_val['tips']
    # val['real_cpi'] = cpi_val['real_cpi']
    # val['pred_cpi'] = cpi_val['pred_cpi']
    # val['past_cpi'] = cpi_val['past_cpi']
    # val['O_vix'] = vix_val['O_vix']
    # val['C_vix'] = vix_val['C_vix']
    # val['H_vix'] = vix_val['H_vix']
    # val['L_vix'] = vix_val['L_vix']
    # val['change_vix'] = vix_val['change_vix']

    # ############################################################################
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

    # test['tips'] = tips_test['tips']
    # test['real_cpi'] = cpi_test['real_cpi']
    # test['pred_cpi'] = cpi_test['pred_cpi']
    # test['past_cpi'] = cpi_test['past_cpi']
    # test['O_vix'] = vix_test['open_vix']
    # test['C_vix'] = vix_test['close_vix']
    # test['H_vix'] = vix_test['high_vix']
    # test['L_vix'] = vix_test['low_vix']
    # test['change_vix'] = vix_test['change_vix']

    ###################################################################
    train = train.fillna(method='bfill')
    val = val.fillna(method='bfill')
    test = test.fillna(method='bfill')
    train = train.fillna(method='ffill')
    val = val.fillna(method='ffill')
    test = test.fillna(method='ffill')

    from sklearn.preprocessing import StandardScaler
        
    if cfg.base.mode=='train':
        scaler = StandardScaler()
        
        df = pd.concat([train, val])
        
        scaled_df = scaler.fit_transform(df)
        scaled_test = scaler.transform(test)
        joblib.dump(scaler, opj(cfg.base.output_dir, 'nasdaq_scaler.pkl'))
    else:
        scaler = joblib.load(opj(cfg.base.output_dir,'nasdaq_scaler.pkl'))
        df = pd.concat([train, val])#.drop("date", axis=1)
        
        dates = pd.to_datetime(df['date'])
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
    pred_days = 30  # prediction period - 3months
    seq_len = 50  # sequence length = past days for future prediction.
    # input_dim = 34  # input_dimension = ['close', 'open', 'high', 'low', 'rsi', 'MACD_12_26', 'MACD_sign_12_26', 'hband', 'mavg', 'lband', 'CSI', 'target']
    input_dim = train.shape[1]

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

    if cfg.base.mode=='train':
        model = Sequential()
        model.add(LSTM(units=20, return_sequences=True, input_shape=(trainX.shape[1], trainX.shape[2])))
        model.add(Dropout(0.1))
        model.add(LSTM(units=20))
        model.add(Dropout(0.1))
        model.add(Dense(units=1))
        model.compile(optimizer="Adam", loss='mse', metrics=['mae'])
        model.fit(trainX, trainY, epochs=30, batch_size=20, validation_data=(valX, valY))
        # joblib.dump(model, opj(cfg.base.output_dir,'nasdaq_lstm.pkl'))
        model.save(opj(cfg.base.output_dir,"nasdaq_lstm.h5"))

    else:

        # model = joblib.load(opj(cfg.base.output_dir,'nasdaq_lstm.pkl'))
        from keras.models import load_model
        model = load_model(opj(cfg.base.output_dir,"nasdaq_lstm.h5"))
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
        pd.DataFrame(data={"date":val_dates_for_infer, "us":val_pred.reshape(-1,)}).to_csv(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_21.csv"), index=False)

        pd.DataFrame(data={"date":test_dates_for_infer, "us":test_pred.reshape(-1,)}).to_csv(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_22.csv"), index=False)
       

