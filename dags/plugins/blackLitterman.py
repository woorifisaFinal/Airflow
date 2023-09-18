'''17~20년의 수익률 데이터를 기반으로 현재 시점에서 30일간 보유하고 있을 때 최적의 포트폴리오 전략을 도출하는 전통적 기법(블랙리터만)
view = 전망에 들어가는 데이터는 현재 시점에서 30일 후 수익률을 예측한 데이터입니다. '''


#Usual Suspects
import pandas as pd
import numpy as np


# Use PyPortfolioOpt for Calculations
from pypfopt import EfficientFrontier, objective_functions
from pypfopt import black_litterman, risk_models
from pypfopt import BlackLittermanModel, plotting
from pypfopt import DiscreteAllocation

# from stage1 import getPrediction

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
import json

def runBlack():
  asset_market = {
      'us': 40719661.400,
      'uk': 2949.56, 
      'jp': 6017876.009,
      'euro': 5558284.855,
      'kor': 1993126.619,
      'ind': 3728884.848,
      'tw': 1712611.47,
      'br': 959887.723,
      'kor3y': 22.66,
      'kor10y': 8.035,
      'us3y': 26.03,
      'us10y': 22.935,
      'gold': 12996.0
      } # 시가총액 (23년 7월 기준)

  col_list = ['us', 'uk', 'jp', 'euro', 'kor', 'ind', 'tw', 'br', 'kor3y', 'kor10y', 'us3y', 'us10y', 'gold'] # list(asset_market.keys())

  rename_dict = {
      'kospi':"kor",
      'nasdaq':"us",
      'euro_stoxx':"euro",
      'ftse':"uk",
      'nikkei':"jp",
      'korea_bond_03':"kor3y",
      'korea_bond_10':"kor10y",
      'america_bond_03':"us3y",
      'america_bond_10':"us10y",
      'gold':"gold",
      'brazil':"br",
      'taiwan':"tw",
      'india':"ind"
      }
  
  def get_portion(asset_market):
    sum_value = sum(asset_market.values())
    for k, v in asset_market.items():
        asset_market[k] = v/sum_value
        
    return pd.Series(asset_market)


  def get_excess_returns():
      # 2022년 수익률
      original_closes_data = pd.read_csv("/opt/airflow/data/close.csv", index_col=0)
      # index가 있어서 col_list 먼저 넣어주기
      excess_returns = original_closes_data[col_list].pct_change().dropna(axis=0) # 자산 수익률 - 무위험 수익률 => 초과 수익률, but 무위험 수익률을 꼭 연산하지 않아도 된다.
      # 종가들을 opt/airflow에 저장할 때 이미 1년값 모두 가져오기 때문에 설정 필요 없음 
      # condition = ("2021-11-30"<=excess_returns.index ) * (excess_returns.index <"2022-11-30")
      # excess_returns = excess_returns[condition]
      # column 명 제대로 되어 있는 RDS에서 가져오는 것이므로 rename 필요 없다.
      # excess_returns.rename(columns=rename_dict, inplace=True)
      # excess_returns = excess_returns[col_list]
      return excess_returns
  
  asset_market = get_portion(asset_market)
  excess_returns = get_excess_returns()
  median = excess_returns.median()

  # Sigma = risk_models.CovarianceShrinkage(excess_returns).ledoit_wolf()
  Sigma = risk_models.CovarianceShrinkage(excess_returns).shrunk_covariance()
  lambd = black_litterman.market_implied_risk_aversion(asset_market)
  market_prior = black_litterman.market_implied_prior_returns(asset_market, lambd, Sigma)

  
  viewdict = getPrediction().mean()


  # bl = BlackLittermanModel(Sigma, pi=market_prior, absolute_views=viewdict)
  bl = BlackLittermanModel(Sigma, pi="market", absolute_views=viewdict,market_caps=asset_market, risk_aversion=lambd)
  ret_bl = bl.bl_returns()
  # rets_df = pd.DataFrame([market_prior, ret_bl, pd.Series(median)],
  #             index=["Prior", "Posterior", "Views"]).T
  S_bl = bl.bl_cov()

  ef = EfficientFrontier(ret_bl, S_bl)
  ef.add_objective(objective_functions.L2_reg)
  ef.max_sharpe()
  weights = ef.clean_weights()
#   weights = weights.astype('float')
#   weights['type'] = 'B/공격형'
#   weights['type'] = weights['type'].astype(str)

  ef_ = EfficientFrontier(ret_bl, S_bl)
  ef_.min_volatility()
  weights_ = ef_.clean_weights()
#   weights_ = weights.astype('float')
#   weights_['type'] = 'B/안정형'
#   weights_['type'] = weights_['type'].astype(str)

  result1 = json.dumps(weights, ensure_ascii=False, sort_keys=False)
  result2 = json.dumps(weights_, ensure_ascii=False, sort_keys=False)
  return result1, result2
