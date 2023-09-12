
## 기존 라이브러리
import pandas as pd


## 데이터 불러오기 위한 class
from stage1.data import DataPreprocess

## 모델 불러오기 위한 함수
from stage1.models import build_rnn_model
from stage1.utils import get_logger

## 모델 불러오기 위한 함수
# from stage1.models import create_jw_model
# from ..models import nasdaq
from stage1 import models
import pickle
from os.path import join as opj

## 환경설정 관련 라이브러리
import argparse
import importlib
import yaml
from types import SimpleNamespace
import sys

# pyyaml docs : https://pyyaml.org/wiki/PyYAMLDocumentation
# Ref : https://github.com/ybabakhin/kaggle-feedback-effectiveness-1st-place-solution/blob/main/train.py
### 예시로 살펴볼 수 있는 yaml 파일들 
# https://github.com/PaddlePaddle/PaddleOCR/blob/release/2.6/configs/cls/cls_mv3.yml
# Ref : https://github.com/ybabakhin/kaggle-feedback-effectiveness-1st-place-solution/blob/main/yaml/awesome-rose-ff.yaml

## 환경설정 얻어오기 위해 인자 받기
parser = argparse.ArgumentParser(description="")
parser.add_argument("-C", "--config", help="config filename")
parser.add_argument("-D", "--date", help="today date format : YYYYMMDD")
parser_args, _ = parser.parse_known_args(sys.argv)
cfg = yaml.safe_load(open(parser_args.config).read())
for k, v in cfg.items():
    if type(v) == dict:
        cfg[k] = SimpleNamespace(**v)
cfg = SimpleNamespace(**cfg)
# print(cfg)

from datetime import datetime

cfg.base.base_date = datetime.strptime(parser_args.date, '%Y%m%d').strftime('%Y-%m-%d')


def infer(cfg):
    """
    데이터 -> 모델 -> 추론 -> (예측 결과) 저장
    """
    if cfg.base.user_name == "jw":
        
        if cfg.base.model_name == "LSTM":
            
            ##### 데이터 불러오기 + 모델 불러오기 
            models.create_jw_lstm(cfg)

        elif cfg.base.model_name == "xgboost":

            models.create_jw_xgboost(cfg)
        
    elif cfg.base.user_name == "sm":

        if cfg.base.task_name == "nikkei":

            if cfg.base.model_name == "LSTM":
                models.nikkei_lstm(cfg)
            
            elif cfg.base.model_name == "xgboost":
                models.nikkei_xgb(cfg)
                
        elif cfg.base.task_name == 'ftse':
            if cfg.base.model_name == "LSTM":
                models.ftse_lstm(cfg)
            elif cfg.base.model_name == "xgboost":
                models.ftse_xgb(cfg)
        else:
            if cfg.base.model_name == "LSTM":
                # models.nasdaq_lstm(cfg)
                models.nasdaq_lstm(cfg)
                # nasdaq.nasdaq_lstm(cfg)
            elif cfg.base.model_name == "xgboost":
                models.nasdaq_xgb(cfg)
    elif cfg.base.user_name == "tw":

        if cfg.base.task_name == "kor3y":

            if cfg.base.model_name == "LSTM":
                models.bond_short(cfg)
            
            elif cfg.base.model_name == "xgboost":
                models.xgb_bond_short(cfg)
                
        elif cfg.base.task_name == "kor10y":

            if cfg.base.model_name == "LSTM":
                models.bond_long(cfg)
            
            elif cfg.base.model_name == "xgboost":
                models.xgb_bond_long(cfg)
        elif cfg.base.task_name == "us3y":

            if cfg.base.model_name == "LSTM":
                models.us_bond_short(cfg)
            
            elif cfg.base.model_name == "xgboost":
                models.xgb_us_bond_short(cfg)
                

        elif cfg.base.task_name == "us10y":

            if cfg.base.model_name == "LSTM":
                models.us_bond_long(cfg)
            
            elif cfg.base.model_name == "xgboost":
                models.xgb_us_bond_long(cfg)
                
 
        
    elif cfg.base.user_name == "jh":
        
        logger = get_logger(cfg.base)
        if cfg.base.mode=='valid':
            x_data, y_data, date_list = DataPreprocess(cfg).load_data(logger)
        elif cfg.base.mode=='infer':
            x_data = DataPreprocess(cfg).load_data(logger)
        
        model = build_rnn_model(cfg)
        model.load_weights(opj(cfg.base.output_dir, f"{cfg.base.task_name}_{cfg.base.model_name}_{cfg.base.exp_name}.h5"))
        if cfg.base.mode=='valid':
                
            import numpy as np
            ##### 예측 & 결과 저장
            preds = np.zeros(y_data.shape)



            preds = model.predict(x_data, verbose="auto") # / FOLDS

            # 추가.. 예측 결과 저장 (stage2에서 쓰도록)
            import pickle
            pd.DataFrame(data={"date":date_list, cfg.base.task_name:preds.reshape(-1,)}).to_csv(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_22.csv"), index=False)

        elif cfg.base.mode=='infer':
            preds = model.predict(x_data, verbose="auto")
            pd.DataFrame(data={"date":cfg.base.base_date, cfg.base.task_name:preds.reshape(-1,)}).to_csv(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_{cfg.base.base_date}.csv"), index=False)
    

    elif cfg.base.user_name == "bg":
        models.gold_lstm(cfg)
    elif cfg.base.user_name == "hs":
        if cfg.base.model_name == "LSTM":
            models.euro_lstm(cfg)
        
if __name__=="__main__":
    infer(cfg)