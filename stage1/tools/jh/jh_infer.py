
## 기존 라이브러리
import pandas as pd

## 데이터 불러오기 위한 class
from stage1.data import DataPreprocess

## 모델 불러오기 위한 함수
from stage1.models import build_rnn_model

from os.path import join as opj
from stage1.utils import get_logger

## 환경설정 관련 라이브러리
import argparse
import importlib
import yaml
from types import SimpleNamespace
import sys
import numpy as np

# pyyaml docs : https://pyyaml.org/wiki/PyYAMLDocumentation
# Ref : https://github.com/ybabakhin/kaggle-feedback-effectiveness-1st-place-solution/blob/main/train.py
### 예시로 살펴볼 수 있는 yaml 파일들 
# https://github.com/PaddlePaddle/PaddleOCR/blob/release/2.6/configs/cls/cls_mv3.yml
# Ref : https://github.com/ybabakhin/kaggle-feedback-effectiveness-1st-place-solution/blob/main/yaml/awesome-rose-ff.yaml

## 환경설정 얻어오기 위해 인자 받기
parser = argparse.ArgumentParser(description="")
parser.add_argument("-C", "--config", help="config filename")

parser_args, _ = parser.parse_known_args(sys.argv)
cfg = yaml.safe_load(open(parser_args.config).read())
for k, v in cfg.items():
    if type(v) == dict:
        cfg[k] = SimpleNamespace(**v)
cfg = SimpleNamespace(**cfg)
# print(cfg)

logger = get_logger(cfg.base)

def infer(cfg):
    """
    데이터 -> 모델 -> 추론 -> (예측 결과) 저장
    """
    ##### 데이터 불러오기

    x_data, y_data = DataPreprocess(cfg).load_data(logger)

    ##### 모델 불러오기

    model = build_rnn_model(cfg)
    model.load_weights(f"{cfg.base.stage_name}_{cfg.base.model_name}_{cfg.base.exp_name}.h5")

    ##### 예측 & 결과 저장
    preds = np.zeros(y_data.shape)



    preds = model.predict(x_data, verbose=2) # / FOLDS

    # 추가.. 예측 결과 저장 (stage2에서 쓰도록)
    import pickle
    with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_22.pkl"), 'wb') as f:
        pickle.dump(preds, f)
        

if __name__=="__main__":
    infer(cfg)