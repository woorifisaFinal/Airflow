import numpy as np
import pandas as pd
from pandas_datareader import data as pdr
import matplotlib.pyplot as plt


gold = pd.read_csv('raw_data\XAU_USD_금_일간.csv')
philadelphia_gold = pd.read_csv('raw_data/Philadelphia Gold_일간.csv')
nem = pd.read_csv('raw_data/NEM_일간.csv')
dollar = pd.read_csv('raw_data/달러_지수_일간.csv')
nasdaq = pd.read_csv('raw_data/나스닥종합지수 과거 데이터.csv')
silver = pd.read_csv('raw_data/은_선물_과거_데이터.csv')
dj = pd.read_csv('raw_data/다우존스_2017_2022 (1).csv')
dax = pd.read_csv('raw_data/DAX_2017_2022.csv')
eu50 = pd.read_csv('raw_data/유로 스톡스50_2017_2022 (1).csv')
sp = pd.read_csv('raw_data/S&P 500_2017_2022 (1).csv')
WTI= pd.read_csv('raw_data/WTI유 선물 과거 데이터.csv')
cow= pd.read_csv('raw_data/육우 선물 과거 데이터.csv')
cu= pd.read_csv('raw_data/구리 선물 과거 데이터.csv')

"""#데이터 전처리"""

# @title
#,표 없애기
gold['종가']=gold['종가'].str.replace(',','')
nasdaq['종가'] = nasdaq['종가'].str.replace(',','')
dj['종가'] = dj['종가'].str.replace(',','')
eu50['종가'] = eu50['종가'].str.replace(',','')
sp['종가'] = sp['종가'].str.replace(',','')

def remove_commas_and_convert(value):
    try:
        return pd.to_numeric(value.replace(',', ''))
    except:
        return value

gold = gold.applymap(remove_commas_and_convert)
philadelphia_gold = philadelphia_gold.applymap(remove_commas_and_convert)
nem = nem.applymap(remove_commas_and_convert)
dollar = dollar.applymap(remove_commas_and_convert)
nasdaq = nasdaq.applymap(remove_commas_and_convert)
silver = silver.applymap(remove_commas_and_convert)
dj = dj.applymap(remove_commas_and_convert)
dax = dax.applymap(remove_commas_and_convert)
eu50 = eu50.applymap(remove_commas_and_convert)
sp = sp.applymap(remove_commas_and_convert)
WTI = WTI.applymap(remove_commas_and_convert)
cow = cow.applymap(remove_commas_and_convert)
cu = cu.applymap(remove_commas_and_convert)

def remove_commas_and_convert2(value):
    try:
        return pd.to_numeric(value.replace('%', ''))
    except:
        return value

def remove_commas_and_convert3(value):
    try:
        return pd.to_numeric(value.replace('K', ''))
    except:
        return value

def remove_commas_and_convert4(value):
    try:
        return pd.to_numeric(value.replace('M', ''))
    except:
        return value

def remove_commas_and_convert5(value):
    try:
        return pd.to_numeric(value.replace('B', ''))
    except:
        return value

gold = gold.applymap(remove_commas_and_convert2)
philadelphia_gold = philadelphia_gold.applymap(remove_commas_and_convert2)
nem = nem.applymap(remove_commas_and_convert2)
dollar = dollar.applymap(remove_commas_and_convert2)
nasdaq = nasdaq.applymap(remove_commas_and_convert2)
silver = silver.applymap(remove_commas_and_convert2)
dj = dj.applymap(remove_commas_and_convert2)
dax = dax.applymap(remove_commas_and_convert2)
eu50 = eu50.applymap(remove_commas_and_convert2)
sp = sp.applymap(remove_commas_and_convert2)
WTI = WTI.applymap(remove_commas_and_convert2)
cow = cow.applymap(remove_commas_and_convert2)
cu = cu.applymap(remove_commas_and_convert2)

gold = gold.applymap(remove_commas_and_convert3)
philadelphia_gold = philadelphia_gold.applymap(remove_commas_and_convert3)
nem = nem.applymap(remove_commas_and_convert3)
dollar = dollar.applymap(remove_commas_and_convert3)
nasdaq = nasdaq.applymap(remove_commas_and_convert3)
silver = silver.applymap(remove_commas_and_convert3)
dj = dj.applymap(remove_commas_and_convert3)
dax = dax.applymap(remove_commas_and_convert3)
eu50 = eu50.applymap(remove_commas_and_convert3)
sp = sp.applymap(remove_commas_and_convert3)
WTI = WTI.applymap(remove_commas_and_convert3)
cow = cow.applymap(remove_commas_and_convert3)
cu = cu.applymap(remove_commas_and_convert3)

gold = gold.applymap(remove_commas_and_convert4)
philadelphia_gold = philadelphia_gold.applymap(remove_commas_and_convert4)
nem = nem.applymap(remove_commas_and_convert4)
dollar = dollar.applymap(remove_commas_and_convert4)
nasdaq = nasdaq.applymap(remove_commas_and_convert4)
silver = silver.applymap(remove_commas_and_convert4)
dj = dj.applymap(remove_commas_and_convert4)
dax = dax.applymap(remove_commas_and_convert4)
eu50 = eu50.applymap(remove_commas_and_convert4)
sp = sp.applymap(remove_commas_and_convert4)
WTI = WTI.applymap(remove_commas_and_convert4)
cow = cow.applymap(remove_commas_and_convert4)
cu = cu.applymap(remove_commas_and_convert4)

gold = gold.applymap(remove_commas_and_convert5)
philadelphia_gold = philadelphia_gold.applymap(remove_commas_and_convert5)
nem = nem.applymap(remove_commas_and_convert5)
dollar = dollar.applymap(remove_commas_and_convert5)
nasdaq = nasdaq.applymap(remove_commas_and_convert5)
silver = silver.applymap(remove_commas_and_convert5)
dj = dj.applymap(remove_commas_and_convert5)
dax = dax.applymap(remove_commas_and_convert5)
eu50 = eu50.applymap(remove_commas_and_convert5)
sp = sp.applymap(remove_commas_and_convert5)
WTI = WTI.applymap(remove_commas_and_convert5)
cow = cow.applymap(remove_commas_and_convert5)
cu = cu.applymap(remove_commas_and_convert5)

# @title
gold['날짜'] = pd.to_datetime(gold['날짜'])
philadelphia_gold['날짜'] = pd.to_datetime(philadelphia_gold['날짜'])
nem['날짜'] = pd.to_datetime(nem['날짜'])
dollar['날짜'] = pd.to_datetime(dollar['날짜'])
nasdaq['날짜'] = pd.to_datetime(nasdaq['날짜'])
silver['날짜'] = pd.to_datetime(silver['날짜'])
dj['날짜'] = pd.to_datetime(dj['날짜'])
dax['날짜'] = pd.to_datetime(dax['날짜'])
eu50['날짜'] = pd.to_datetime(eu50['날짜'])
sp['날짜'] = pd.to_datetime(sp['날짜'])
WTI['날짜'] = pd.to_datetime(sp['날짜'])
cow['날짜'] = pd.to_datetime(sp['날짜'])
cu['날짜'] = pd.to_datetime(sp['날짜'])

gold_c = gold
philadelphia_gold_c = philadelphia_gold
nem_c = nem
dollar_c = dollar
nasdaq_c = nasdaq
silver_c = silver

gold_c = pd.merge(gold_c,philadelphia_gold_c, how='left',on='날짜', suffixes=('_gold', '_PH_gold'))

gold_c = pd.merge(gold_c,nem_c, how='left',on='날짜', suffixes=('', '_nem_c'))
gold_c = pd.merge(gold_c,dollar_c, how='left',on='날짜', suffixes=('', '_dollar_c'))
gold_c = pd.merge(gold_c,nasdaq_c, how='left',on='날짜', suffixes=('', '_nasdaq_c'))
gold_c = pd.merge(gold_c,silver_c, how='left',on='날짜', suffixes=('', '_silver_c'))
gold_c = pd.merge(gold_c,dj, how='left',on='날짜', suffixes=('', '_dj'))
gold_c = pd.merge(gold_c,dax, how='left',on='날짜', suffixes=('', '_dax'))
gold_c = pd.merge(gold_c,eu50, how='left',on='날짜', suffixes=('', '_eu50'))
gold_c = pd.merge(gold_c,sp, how='left',on='날짜', suffixes=('', '_sp'))
gold_c = pd.merge(gold_c,WTI, how='left',on='날짜', suffixes=('', '_WTI'))
gold_c = pd.merge(gold_c,cow, how='left',on='날짜', suffixes=('', '_cow'))
gold_c = pd.merge(gold_c,cu, how='left',on='날짜', suffixes=('', '_cu'))

# @title
merge_outer = gold_c

merge_outer.drop(columns='거래량_gold', inplace=True)
merge_outer.drop(columns='거래량_PH_gold', inplace=True)
merge_outer.drop(columns='거래량_dollar_c', inplace=True)
merge_outer.drop(columns='거래량_sp', inplace=True)

merge_outer.set_index('날짜',inplace=True)

merge_outer = merge_outer.astype('float')

merge_outer.reset_index(inplace=True)
merge_outer.rename(columns={'index': '날짜'}, inplace=True)

# @title
merge_outer = merge_outer.sort_values('날짜')

# @title
merge_outer = merge_outer.reset_index(drop=True)

# @title
merge_outer.isnull().sum()

# @title
merge_outer.fillna(method='bfill', inplace=True)

# @title
merge_outer.isnull().sum()

merge_outer['거래량_WTI'].fillna(0, inplace=True)

eda_data= merge_outer

eda_data.to_csv('all_eda_data.csv', index=False)

"""#전처리 완료"""

data_nomalized = eda_data

train = data_nomalized[data_nomalized['날짜'].between('2017-01-01', '2020-12-31')]
vaildation = data_nomalized[data_nomalized['날짜'].between('2021-01-01', '2021-12-31')]
test = data_nomalized[data_nomalized['날짜'].between('2022-01-01', '2022-12-31')]

train = train.reset_index(drop=True)
vaildation = vaildation.reset_index(drop=True)
test = test.reset_index(drop=True)
