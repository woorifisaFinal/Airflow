
from os.path import join as opj

def ftse_xgb(cfg):
  import pandas as pd
  import numpy as np
  import yfinance as yf
  from pandas_datareader import data as pdr
  import ta
  import joblib

  ##FOR LOCAL
  # train = pd.read_csv('data/train_ftse.csv')
  # val = pd.read_csv('data/val_ftse.csv')
  # test = pd.read_csv('data/test_ftse.csv')
  # tips = pd.read_csv('data/tips_uk_2017_2022.csv')
  # cpi = pd.read_csv('data/cpi_uk.csv')
  # euro = pd.read_csv('data/euro_2017_2022.csv')
  # retail = pd.read_csv('data/retail_sales_uk.csv')
  # unemp = pd.read_csv('data/unemployment_uk.csv')
  ##FOR PROJECT
  train = pd.read_csv(opj(cfg.base.data_dir, 'train_ftse.csv'))
  val = pd.read_csv(opj(cfg.base.data_dir, 'val_ftse.csv'))
  test = pd.read_csv(opj(cfg.base.data_dir, 'test_ftse.csv'))
  tips = pd.read_csv(opj(cfg.base.data_dir, 'tips_uk_2017_2022.csv'))
  cpi = pd.read_csv(opj(cfg.base.data_dir, 'cpi_uk.csv'))
  euro = pd.read_csv(opj(cfg.base.data_dir, 'euro_2017_2022.csv'))
  retail = pd.read_csv(opj(cfg.base.data_dir, 'retail_sales_uk.csv'))
  unemp = pd.read_csv(opj(cfg.base.data_dir, 'unemployment_uk.csv'))
  #################################################################
  tips['date'] = pd.to_datetime(tips['date'])
  tips = tips.set_index('date')
  cpi['date'] = pd.to_datetime(cpi['date'])
  cpi = cpi.set_index('date')
  euro['date'] = pd.to_datetime(euro['date'])
  euro = euro.set_index('date')
  retail['date'] = pd.to_datetime(retail['date'])
  retail = retail.set_index('date')
  unemp['date'] = pd.to_datetime(unemp['date'])
  unemp = unemp.set_index('date')

  tips = tips.rename(columns={'close': 'C_tips', 'open': 'O_tips', 'high': 'H_tips',
                              'low': 'L_tips', 'change': 'change_tips'})
  euro = euro.rename(columns={'close': 'C_euro', 'open': 'O_euro', 'high': 'H_euro',
                              'low': 'L_euro', 'volume': 'V_euro', 'change': 'change_euro'})


  #####
  train_end = '2020-12-31'
  val_start = '2021-01-04'
  val_end = '2021-12-31'
  test_start = '2022-01-01'
  test_end = '2022-12-31'

  tips_train = tips[:train_end]
  tips_val = tips[val_start:val_end]
  tips_test = tips[test_start:test_end]

  cpi_train = cpi[:train_end]
  cpi_val = cpi[val_start: val_end]
  cpi_test = cpi[test_start:test_end]

  unemp_train = unemp[:train_end]
  unemp_val = unemp[val_start: val_end]
  unemp_test = unemp[test_start:test_end]

  euro_train = euro[:train_end]
  euro_val = euro[val_start: val_end]
  euro_test = euro[test_start:test_end]

  rs_train = retail[:train_end]
  rs_val = retail[val_start: val_end]
  rs_test = retail[test_start:test_end]

  ########3
  H, L, C, V = train['high'], train['low'], train['close'], train['volume']
  # 수익률 = target
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
  # train['year'] = train.index.year
  train['dayofweek'] = train.index.dayofweek

  # 'C_tips''O_tips''H_tips''L_tips''change_tips'
  # 'C_euro''O_euro''H_euro''L_euro''V_euro''change_vix'
  # 'real_unemp''pred_unemp''past_unemp'
  # 'real_rs''pred_rs''past_rs'
  # 'real_cpi''pred_cpi''past_cpi'

  train['C_tips'] = tips_train['C_tips']
  train['O_tips'] = tips_train['O_tips']
  train['H_tips'] = tips_train['H_tips']
  train['L_tips'] = tips_train['L_tips']
  train['change_tips'] = tips_train['change_tips']

  train['C_euro'] = euro_train['C_euro']
  train['O_euro'] = euro_train['O_euro']
  train['H_euro'] = euro_train['H_euro']
  train['L_euro'] = euro_train['L_euro']
  train['V_euro'] = euro_train['V_euro']
  train['change_euro'] = euro_train['change_euro']

  train['real_cpi'] = cpi_train['real_cpi']
  train['pred_cpi'] = cpi_train['pred_cpi']
  train['past_cpi'] = cpi_train['past_cpi']

  train['real_rs'] = rs_train['real_rs']
  train['pred_rs'] = rs_train['pred_rs']
  train['past_rs'] = rs_train['past_rs']

  train['real_unemp'] = unemp_train['real_unemp']
  train['pred_unemp'] = unemp_train['pred_unemp']
  train['past_unemp'] = unemp_train['past_unemp']

  H_val, L_val, C_val, V_val = val['high'], val['low'], val['close'], val['volume']
  # 수익률 = target
  val['target'] = val['close'].pct_change()
  # val['target'].fillna(method='bfill')
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
  # val['year'] = val.index.year
  val['dayofweek'] = val.index.dayofweek

  val['C_tips'] = tips_val['C_tips']
  val['O_tips'] = tips_val['O_tips']
  val['H_tips'] = tips_val['H_tips']
  val['L_tips'] = tips_val['L_tips']
  val['change_tips'] = tips_val['change_tips']

  val['C_euro'] = euro_val['C_euro']
  val['O_euro'] = euro_val['O_euro']
  val['H_euro'] = euro_val['H_euro']
  val['L_euro'] = euro_val['L_euro']
  val['V_euro'] = euro_val['V_euro']
  val['change_euro'] = euro_val['change_euro']

  val['real_cpi'] = cpi_val['real_cpi']
  val['pred_cpi'] = cpi_val['pred_cpi']
  val['past_cpi'] = cpi_val['past_cpi']

  val['real_rs'] = rs_val['real_rs']
  val['pred_rs'] = rs_val['pred_rs']
  val['past_rs'] = rs_val['past_rs']

  val['real_unemp'] = unemp_val['real_unemp']
  val['pred_unemp'] = unemp_val['pred_unemp']
  val['past_unemp'] = unemp_val['past_unemp']

  H_test, L_test, C_test, V_test = test['high'], test['low'], test['close'], test['volume']
  # 수익률 = target
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
  # val['year'] = val.index.year
  test['dayofweek'] = test.index.dayofweek

  test['C_tips'] = tips_test['C_tips']
  test['O_tips'] = tips_test['O_tips']
  test['H_tips'] = tips_test['H_tips']
  test['L_tips'] = tips_test['L_tips']
  test['change_tips'] = tips_test['change_tips']

  test['C_euro'] = euro_test['C_euro']
  test['O_euro'] = euro_test['O_euro']
  test['H_euro'] = euro_test['H_euro']
  test['L_euro'] = euro_test['L_euro']
  test['V_euro'] = euro_test['V_euro']
  test['change_euro'] = euro_test['change_euro']

  test['real_cpi'] = cpi_test['real_cpi']
  test['pred_cpi'] = cpi_test['pred_cpi']
  test['past_cpi'] = cpi_test['past_cpi']

  test['real_rs'] = rs_test['real_rs']
  test['pred_rs'] = rs_test['pred_rs']
  test['past_rs'] = rs_test['past_rs']

  test['real_unemp'] = unemp_test['real_unemp']
  test['pred_unemp'] = unemp_test['pred_unemp']
  test['past_unemp'] = unemp_test['past_unemp']


  train = train.fillna(method='bfill')
  val = val.fillna(method='bfill')
  test = test.fillna(method='bfill')
  train = train.fillna(method='ffill')
  val = val.fillna(method='ffill')
  test = test.fillna(method='ffill')

  X_train = pd.DataFrame(train, columns=['month', 'day', 'dayofweek', 'high', 'low',
                                         'volume', 'ATR', 'Parabolic SAR', 'MACD', 'SMA', 'EMA', 'RSI',
                                         'C_tips', 'O_tips', 'H_tips', 'L_tips', 'change_tips',
                                         'C_euro', 'O_euro', 'H_euro', 'L_euro', 'V_euro', 'change_vix',
                                         'real_unemp', 'pred_unemp', 'past_unemp',
                                         'real_rs', 'pred_rs', 'past_rs',
                                         'real_cpi', 'pred_cpi', 'past_cpi',
                                         'close'])
  y_train = train['target']
  X_val = pd.DataFrame(val, columns=['month', 'day', 'dayofweek', 'high', 'low',
                                     'volume', 'ATR', 'Parabolic SAR', 'MACD', 'SMA', 'EMA', 'RSI',
                                     'C_tips', 'O_tips', 'H_tips', 'L_tips', 'change_tips',
                                     'C_euro', 'O_euro', 'H_euro', 'L_euro', 'V_euro', 'change_vix',
                                     'real_unemp', 'pred_unemp', 'past_unemp',
                                     'real_rs', 'pred_rs', 'past_rs',
                                     'real_cpi', 'pred_cpi', 'past_cpi',
                                     'close'])
  y_val = val['target']
  X_test = pd.DataFrame(test, columns=['month', 'day', 'dayofweek', 'high', 'low',
                                       'volume', 'ATR', 'Parabolic SAR', 'MACD', 'SMA', 'EMA', 'RSI',
                                       'C_tips', 'O_tips', 'H_tips', 'L_tips', 'change_tips',
                                       'C_euro', 'O_euro', 'H_euro', 'L_euro', 'V_euro', 'change_vix',
                                       'real_unemp', 'pred_unemp', 'past_unemp',
                                       'real_rs', 'pred_rs', 'past_rs',
                                       'real_cpi', 'pred_cpi', 'past_cpi',
                                       'close'])

  y_test = test['target']

  from xgboost import XGBRegressor
  xgb_model = XGBRegressor(n_estimators=1000, max_depth=5)

  xgb_model.fit(X_train, y_train, eval_metric='rmse', eval_set=[(X_val, y_val)])
  joblib.dump(xgb_model, opj(cfg.base.output_dir, 'models/ftse_xgb.pkl'))


def ftse_lstm(cfg):

    import pandas as pd
    import numpy as np
    import yfinance as yf
    from pandas_datareader import data as pdr
    import ta
    import joblib

    if cfg.base.mode=='infer':

        ## FOR PROJECT
        train = pd.read_csv(opj(cfg.base.data_dir, 'train_ftse.csv'))
        val = pd.read_csv(opj(cfg.base.data_dir, 'val_ftse.csv'))
        test = pd.read_csv(opj(cfg.base.data_dir, 'test_ftse.csv'))

        total_df = pd.concat([train, val, test])
        dates = pd.to_datetime(total_df['date'])
        
        if (total_df['date']==cfg.base.base_date).sum()==0:
            # 존재하지 않는 날. 휴장
            pd.DataFrame(data={"date":cfg.base.base_date, "uk":np.NaN},index=[0]).to_csv(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_{cfg.base.base_date}.csv"), index=False)
            return 
        else:
            total_df = total_df.fillna(method='bfill')
            total_df = total_df.fillna(method='ffill')
            test = total_df[total_df['date']<cfg.base.base_date].tail(50)
    elif cfg.base.mode=='train':
        ##FOR LOCAL
        # train = pd.read_csv('data/train_ftse.csv')
        # val = pd.read_csv('data/val_ftse.csv')
        # test = pd.read_csv('data/test_ftse.csv')
        # tips = pd.read_csv('data/tips_uk_2017_2022.csv')
        # cpi = pd.read_csv('data/cpi_uk.csv')
        # euro = pd.read_csv('data/euro_2017_2022.csv')
        # retail = pd.read_csv('data/retail_sales_uk.csv')
        # unemp = pd.read_csv('data/unemployment_uk.csv')
        ##FOR PROJECT
        train = pd.read_csv(opj(cfg.base.data_dir, 'train_ftse.csv'))
        val = pd.read_csv(opj(cfg.base.data_dir, 'val_ftse.csv'))
        test = pd.read_csv(opj(cfg.base.data_dir, 'test_ftse.csv'))
        # tips = pd.read_csv(opj(cfg.base.data_dir, 'tips_uk_2017_2022.csv'))
        # cpi = pd.read_csv(opj(cfg.base.data_dir, 'cpi_uk.csv'))
        # euro = pd.read_csv(opj(cfg.base.data_dir, 'euro_2017_2022.csv'))
        # retail = pd.read_csv(opj(cfg.base.data_dir, 'retail_sales_uk.csv'))
        # unemp = pd.read_csv(opj(cfg.base.data_dir, 'unemployment_uk.csv'))
        #################################################################
        _ = pd.concat([train, val, test])
        dates = pd.to_datetime(_['date'])


        # tips['date'] = pd.to_datetime(tips['date'])
        # tips = tips.set_index('date')
        # cpi['date'] = pd.to_datetime(cpi['date'])
        # cpi = cpi.set_index('date')
        # euro['date'] = pd.to_datetime(euro['date'])
        # euro = euro.set_index('date')
        # retail['date'] = pd.to_datetime(retail['date'])
        # retail = retail.set_index('date')
        # unemp['date'] = pd.to_datetime(unemp['date'])
        # unemp = unemp.set_index('date')

        # tips = tips.rename(columns={'close': 'C_tips', 'open': 'O_tips', 'high': 'H_tips',
        #                             'low': 'L_tips', 'change': 'change_tips'})
        # euro = euro.rename(columns={'close': 'C_euro', 'open': 'O_euro', 'high': 'H_euro',
        #                             'low': 'L_euro', 'volume': 'V_euro', 'change': 'change_euro'})

        # #####
        train_end = '2020-12-31'
        val_start = '2021-01-04'
        val_end = '2021-12-31'
        test_start = '2022-01-01'
        test_end = '2022-12-31'

        # tips_train = tips[:train_end]
        # tips_val = tips[val_start:val_end]
        # tips_test = tips[test_start:test_end]

        # cpi_train = cpi[:train_end]
        # cpi_val = cpi[val_start: val_end]
        # cpi_test = cpi[test_start:test_end]

        # unemp_train = unemp[:train_end]
        # unemp_val = unemp[val_start: val_end]
        # unemp_test = unemp[test_start:test_end]

        # euro_train = euro[:train_end]
        # euro_val = euro[val_start: val_end]
        # euro_test = euro[test_start:test_end]

        # rs_train = retail[:train_end]
        # rs_val = retail[val_start: val_end]
        # rs_test = retail[test_start:test_end]

        # ########3
        # H, L, C, V = train['high'], train['low'], train['close'], train['volume']
        # # 수익률 = target
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
        # # train['year'] = train.index.year
        # train['dayofweek'] = train.index.dayofweek

        # # 'C_tips''O_tips''H_tips''L_tips''change_tips'
        # # 'C_euro''O_euro''H_euro''L_euro''V_euro''change_vix'
        # # 'real_unemp''pred_unemp''past_unemp'
        # # 'real_rs''pred_rs''past_rs'
        # # 'real_cpi''pred_cpi''past_cpi'

        # train['C_tips'] = tips_train['C_tips']
        # train['O_tips'] = tips_train['O_tips']
        # train['H_tips'] = tips_train['H_tips']
        # train['L_tips'] = tips_train['L_tips']
        # train['change_tips'] = tips_train['change_tips']

        # train['C_euro'] = euro_train['C_euro']
        # train['O_euro'] = euro_train['O_euro']
        # train['H_euro'] = euro_train['H_euro']
        # train['L_euro'] = euro_train['L_euro']
        # train['V_euro'] = euro_train['V_euro']
        # train['change_euro'] = euro_train['change_euro']

        # train['real_cpi'] = cpi_train['real_cpi']
        # train['pred_cpi'] = cpi_train['pred_cpi']
        # train['past_cpi'] = cpi_train['past_cpi']

        # train['real_rs'] = rs_train['real_rs']
        # train['pred_rs'] = rs_train['pred_rs']
        # train['past_rs'] = rs_train['past_rs']

        # train['real_unemp'] = unemp_train['real_unemp']
        # train['pred_unemp'] = unemp_train['pred_unemp']
        # train['past_unemp'] = unemp_train['past_unemp']

        # H_val, L_val, C_val, V_val = val['high'], val['low'], val['close'], val['volume']
        # # 수익률 = target
        # val['target'] = val['close'].pct_change()
        # # val['target'].fillna(method='bfill')
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
        # # val['year'] = val.index.year
        # val['dayofweek'] = val.index.dayofweek

        # val['C_tips'] = tips_val['C_tips']
        # val['O_tips'] = tips_val['O_tips']
        # val['H_tips'] = tips_val['H_tips']
        # val['L_tips'] = tips_val['L_tips']
        # val['change_tips'] = tips_val['change_tips']

        # val['C_euro'] = euro_val['C_euro']
        # val['O_euro'] = euro_val['O_euro']
        # val['H_euro'] = euro_val['H_euro']
        # val['L_euro'] = euro_val['L_euro']
        # val['V_euro'] = euro_val['V_euro']
        # val['change_euro'] = euro_val['change_euro']

        # val['real_cpi'] = cpi_val['real_cpi']
        # val['pred_cpi'] = cpi_val['pred_cpi']
        # val['past_cpi'] = cpi_val['past_cpi']

        # val['real_rs'] = rs_val['real_rs']
        # val['pred_rs'] = rs_val['pred_rs']
        # val['past_rs'] = rs_val['past_rs']

        # val['real_unemp'] = unemp_val['real_unemp']
        # val['pred_unemp'] = unemp_val['pred_unemp']
        # val['past_unemp'] = unemp_val['past_unemp']

        # H_test, L_test, C_test, V_test = test['high'], test['low'], test['close'], test['volume']
        # # 수익률 = target
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
        # # val['year'] = val.index.year
        # test['dayofweek'] = test.index.dayofweek

        # test['C_tips'] = tips_test['C_tips']
        # test['O_tips'] = tips_test['O_tips']
        # test['H_tips'] = tips_test['H_tips']
        # test['L_tips'] = tips_test['L_tips']
        # test['change_tips'] = tips_test['change_tips']

        # test['C_euro'] = euro_test['C_euro']
        # test['O_euro'] = euro_test['O_euro']
        # test['H_euro'] = euro_test['H_euro']
        # test['L_euro'] = euro_test['L_euro']
        # test['V_euro'] = euro_test['V_euro']
        # test['change_euro'] = euro_test['change_euro']

        # test['real_cpi'] = cpi_test['real_cpi']
        # test['pred_cpi'] = cpi_test['pred_cpi']
        # test['past_cpi'] = cpi_test['past_cpi']

        # test['real_rs'] = rs_test['real_rs']
        # test['pred_rs'] = rs_test['pred_rs']
        # test['past_rs'] = rs_test['past_rs']

        # test['real_unemp'] = unemp_test['real_unemp']
        # test['pred_unemp'] = unemp_test['pred_unemp']
        # test['past_unemp'] = unemp_test['past_unemp']

        train = train.fillna(method='bfill')
        val = val.fillna(method='bfill')
        test = test.fillna(method='bfill')
        train = train.fillna(method='ffill')
        val = val.fillna(method='ffill')
        test = test.fillna(method='ffill')

        X_train = pd.DataFrame(train, columns=['month', 'day', 'dayofweek', 'high', 'low',
                                                'volume', 'ATR', 'Parabolic SAR', 'MACD', 'SMA', 'EMA', 'RSI',
                                                'C_tips', 'O_tips', 'H_tips', 'L_tips', 'change_tips',
                                                'C_euro', 'O_euro', 'H_euro', 'L_euro', 'V_euro', 'change_vix',
                                                'real_unemp', 'pred_unemp', 'past_unemp',
                                                'real_rs', 'pred_rs', 'past_rs',
                                                'real_cpi', 'pred_cpi', 'past_cpi',
                                                'close'])
        y_train = train['target']
        X_val = pd.DataFrame(val, columns=['month', 'day', 'dayofweek', 'high', 'low',
                                            'volume', 'ATR', 'Parabolic SAR', 'MACD', 'SMA', 'EMA', 'RSI',
                                            'C_tips', 'O_tips', 'H_tips', 'L_tips', 'change_tips',
                                            'C_euro', 'O_euro', 'H_euro', 'L_euro', 'V_euro', 'change_vix',
                                            'real_unemp', 'pred_unemp', 'past_unemp',
                                            'real_rs', 'pred_rs', 'past_rs',
                                            'real_cpi', 'pred_cpi', 'past_cpi',
                                            'close'])
        y_val = val['target']
        X_test = pd.DataFrame(test, columns=['month', 'day', 'dayofweek', 'high', 'low',
                                            'volume', 'ATR', 'Parabolic SAR', 'MACD', 'SMA', 'EMA', 'RSI',
                                            'C_tips', 'O_tips', 'H_tips', 'L_tips', 'change_tips',
                                            'C_euro', 'O_euro', 'H_euro', 'L_euro', 'V_euro', 'change_vix',
                                            'real_unemp', 'pred_unemp', 'past_unemp',
                                            'real_rs', 'pred_rs', 'past_rs',
                                            'real_cpi', 'pred_cpi', 'past_cpi',
                                            'close'])

        y_test = test['target']
        train = train.fillna(method='bfill')
        val = val.fillna(method='bfill')
        test = test.fillna(method='bfill')
        train = train.fillna(method='ffill')
        val = val.fillna(method='ffill')
        test = test.fillna(method='ffill')

    from sklearn.preprocessing import MinMaxScaler, StandardScaler
    if cfg.base.mode =='train':
        scaler = StandardScaler()
        df = pd.concat([train, val])
        scaled_df = scaler.fit_transform(df)
        scaled_test = scaler.transform(test)
        joblib.dump(scaler, opj(cfg.base.output_dir, 'ftse_scaler.pkl'))
    elif cfg.base.mode =='valid':
        scaler = joblib.load(opj(cfg.base.output_dir,'ftse_scaler.pkl'))
        df = pd.concat([train, val])#.drop("date", axis=1)
        dates = df.reset_index(drop=False)['date']
        # fit_transform을 transform으로 변경
        scaled_df = scaler.transform(df)
        scaled_test = scaler.transform(test)
    else:
        scaler = joblib.load(opj(cfg.base.output_dir,'ftse_scaler.pkl'))
        
        test = test.set_index('date')
        # fit_transform을 transform으로 변경
        # scaled_df = scaler.transform(df)
        scaled_test = scaler.transform(test)
    if cfg.base.mode=='infer':
        testX = []
        testX.append(test)
        testX = np.array(testX)
    else:
        n_train = len(train)
        n_val = len(train) + len(val)
        n_test = n_val

        train_data_scaled = scaled_df[0: n_train]
        train_dates = dates[0: n_train]
        

        val_data_scaled = scaled_df[n_train: n_val]
        val_dates = dates[n_train: n_val]

        test_data_scaled = scaled_test[:]
        test_dates = test.reset_index(drop=False)['date']

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
        #### stftime로 바꿨다
        # val_dates, test_dates는 pd.Series로 되어 있고 numpy datetime으로 되어 있다.
        # val_dates = val_dates.astype('string')
        # test_dates = test_dates.astype('string')

        for i in range(seq_len, n_train - pred_days + 1):
            trainX.append(train_data_scaled[i - seq_len:i, 0:train_data_scaled.shape[1]])
            trainY.append(train_data_scaled[i + pred_days - 1:i + pred_days, 0])

        for i in range(seq_len, len(val_data_scaled) - pred_days + 1):
            valX.append(val_data_scaled[i - seq_len:i, 0:val_data_scaled.shape[1]])
            valY.append(val_data_scaled[i + pred_days - 1:i + pred_days, 0])
            # base_dates추가
            val_dates_for_infer.append(val_dates[i])

        for i in range(seq_len, len(test_data_scaled) - pred_days + 1):
            testX.append(test_data_scaled[i - seq_len:i, 0:test_data_scaled.shape[1]])
            testY.append(test_data_scaled[i + pred_days - 1:i + pred_days, 0])
            # base_dates추가
            test_dates_for_infer.append(test_dates[i])

        trainX, trainY = np.array(trainX), np.array(trainY)
        valX, valY = np.array(valX), np.array(valY)
        testX, testY = np.array(testX), np.array(testY)

    from tensorflow.keras import Sequential
    from tensorflow.keras.layers import Dense, LSTM, Dropout
    from tensorflow.keras.optimizers import Adam
    import tensorflow as tf

    if cfg.base.mode =='train':
        model = Sequential()
        model.add(LSTM(units=64, return_sequences=True, input_shape=(trainX.shape[1], trainX.shape[2])))
        model.add(Dropout(0.1))
        model.add(LSTM(units=128))
        model.add(Dense(units=1))
        model.add(Dense(units=1))
        model.add(Dense(units=1))


        earlystopper = tf.keras.callbacks.EarlyStopping(monitor='val_loss', patience=15, verbose=1)
        learning_rate = 0.01
        optimizer = Adam(learning_rate=learning_rate)
        model.compile(optimizer=optimizer, loss='mse', metrics=['mae'])
        model.fit(trainX, trainY, epochs=30, batch_size=20, validation_data=(valX, valY))
        # joblib.dump(model, opj(cfg.base.output_dir, 'models/ftse_lstm.pkl'))
        model.save(opj(cfg.base.output_dir,"ftse_lstm.h5"))

    elif cfg.base.mode=='valid':

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
        model = load_model(opj(cfg.base.output_dir,"ftse_lstm.h5"))
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
        pd.DataFrame(data={"date":val_dates_for_infer, "uk":val_pred.reshape(-1,)}).to_csv(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_21.csv"), index=False)

        pd.DataFrame(data={"date":test_dates_for_infer, "uk":test_pred.reshape(-1,)}).to_csv(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_22.csv"), index=False)
       
    else:
        
        from keras.models import load_model
        model = load_model(opj(cfg.base.output_dir,"ftse_lstm.h5"))
        test_pred = model.predict(testX)
        pd.DataFrame(data={"date":cfg.base.base_date, "uk":test_pred.reshape(-1,)}).to_csv(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_{cfg.base.base_date}.csv"), index=False)
