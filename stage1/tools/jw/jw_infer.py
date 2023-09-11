
## 기존 라이브러리
import pandas as pd

## 데이터 불러오기 위한 class
from stage1.data import DataPreprocess

## 모델 불러오기 위한 함수
from stage1.models import create_jw_model

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

parser_args, _ = parser.parse_known_args(sys.argv)
cfg = yaml.safe_load(open(parser_args.config).read())
for k, v in cfg.items():
    if type(v) == dict:
        cfg[k] = SimpleNamespace(**v)
cfg = SimpleNamespace(**cfg)
# print(cfg)



def infer(cfg):
    """
    데이터 -> 모델 -> 추론 -> (예측 결과) 저장
    """
    ##### 데이터 불러오기

    trainX, trainY, valX, valY, testX, testY  = DataPreprocess(cfg).load_data()

    ##### 모델 불러오기

    model = create_jw_model(trainX, trainY)

    model.load_weights(opj(cfg.base.output_dir, 'lstm_weights_last.h5'))
    print("Loaded model weights from disk")

    ##### 예측 & 결과 저장

    # prediction
    prediction = model.predict(testX)
    print(prediction.shape, testY.shape)

    # 추가.. 예측 결과 저장 (stage2에서 쓰도록)
    import pickle
    with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_22.pkl"), 'wb') as f:
        pickle.dump(prediction, f)
        

if __name__=="__main__":
    infer(cfg)