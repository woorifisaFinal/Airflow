from glob import glob
import pandas as pd
import pickle
import os
import argparse
from os.path import join as opj
from datetime import datetime
#######################
#### output의 index로 기간들 어떻게 넣어줄지 고려해볼 부분..

# opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_22.pkl"),
# file_list = glob(opj("stage1", "output", "*", "*_prediction_*.csv"))
# file_list_21 = glob(opj("stage1", "output", "*", "*_prediction_21.csv"))
# file_list_22 = glob(opj("stage1", "output", "*", "*_prediction_22.csv"))

parser = argparse.ArgumentParser()
parser.add_argument('-D', '--date')
args = parser.parse_args()

date = datetime.strptime(args.date, '%Y%m%d').strftime('%Y-%m-%d')
file_list = glob(opj("stage1", "output", "*", f"*_prediction_{date}.csv"))

# print("file_list :", file_list)
# # 자산별 예측결과를 저장
# data_dict = {}
# for file_name in file_list:
#     # file_name == 자산명칭_prediciton_기간.pkl
#     # with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_22.pkl"), 'rb') as f:
#     with open(file_name, 'rb') as f:
#         prediction = pickle.load(f)
#     base_name = os.path.basename(file_name)
#     # prediction이 1차원의 numpy거나 list일 경우
#     data_dict[base_name.split("_")[0]]=prediction

# import pickle

# # for check
# with open(opj("stage1", "output", "data_dict.pkl"), 'wb') as f:
#     pickle.dump(data_dict, f)
    
# pred_reseult = pd.DataFrame(data_dict)
# # print("Hi~")
# # 이름에 기간 추가
# # pred_reseult.to_csv(f"stage1_{file_name.split("_")[-1].split(".")[0]}_prediction.csv", index=False)


df_sum= pd.DataFrame(data={'date':date}, index=[0])
for li in file_list:
    a = pd.read_csv(li, index_col=0)#.squeeze()
    df_sum[a.columns] = a.values
df_sum.to_csv(opj("stage1", "output", f"stage1_prediction_{date}.csv"), index=False)

# df_sum= pd.DataFrame()
# for li in file_list_21:
#     a = pd.read_csv(li, index_col=0).squeeze()
#     df_sum[a.name] = a
# df_sum.to_csv(opj("stage1", "output", "stage1_prediction_21.csv"), index=True)

# df_sum= pd.DataFrame()
# for li in file_list_22:
#     a = pd.read_csv(li, index_col=0).squeeze()
#     df_sum[a.name] = a
# df_sum.to_csv(opj("stage1", "output", "stage1_prediction_22.csv"), index=True)





##### 가짜 prediction 파일은 아래와 같은 방식으로 생성 (참고만)
# train = pdr.get_data_yahoo(cfg.base.index_name, cfg.train.start_date, cfg.train.end_date).reset_index()
# valid = pdr.get_data_yahoo(cfg.base.index_name, cfg.valid.start_date, cfg.valid.end_date).reset_index()
# test = pdr.get_data_yahoo(cfg.base.index_name, cfg.test.start_date, cfg.test.end_date).reset_index()

# # pred_result = pd.DataFrame(index=valid.Date)
# pred_result = pd.DataFrame(index=test.Date)
# # .to_csv("stage1_prediction_21.csv") # index=True

# asset_name = ["brazil", "india", "taiwan", "nasdaq","japan", "uk","gold","bond3","bond10","kospi","eurostock"]
# data_dict = {}
# for asset in asset_name:
#     pred_result[asset]= np.random.randn(pred_result.shape[0])

# pred_result.to_csv("stage1_prediction_22.csv") # index=True