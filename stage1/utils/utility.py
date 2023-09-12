



import logging
from os.path import join as opj
import numpy as np
import calendar
import random
import os

__all__ = ['seed_everything','get_logger','scaler','get_week_of_month','rmse','smape'] 

def seed_everything(seed):
    random.seed(seed)
    os.environ['PYTHONHASHSEED'] = str(seed)
    np.random.seed(seed)

# paddleocr과 yolox logger 활용법 배우기?
def get_logger(cfg_base):
    
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler1 = logging.StreamHandler()
    handler1.setFormatter(logging.Formatter("%(message)s")) # 시간을 넣어줘야겠어..
    handler2 = logging.FileHandler(filename=opj(cfg_base.output_dir, f"{cfg_base.task_name}_{cfg_base.model_name}_{cfg_base.exp_name}.log"))
    handler2.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler1)
    logger.addHandler(handler2)
    return logger

# logger = get_logger(cfg.base)



### STANDARIZE
def scaler(x_data,y_data=None, cfg_base=None, is_train=False, logger=None):
    """
    만약 필요하다면 feature 이름과 mn, std를 매칭 시켜서 저장해야 할 수도 있겠다. 순서가 항상 동일하다는 것을 보장할 수 없으니.
    이전에 비해 cfg.data.feature_list 바뀌면 바꿔야 한다.

    현재는 y_data scaling하지 않는다.
    """
    if is_train:
        mn = np.mean(x_data, axis=(0,1))
        sd = np.std(x_data, axis=(0,1))

        with open(opj(cfg_base.output_dir, f"{cfg_base.task_name}_{cfg_base.model_name}_{cfg_base.exp_name}_mn.npy"), "wb") as f:
            np.save(f, mn)
        with open(opj(cfg_base.output_dir, f"{cfg_base.task_name}_{cfg_base.model_name}_{cfg_base.exp_name}_sd.npy"), "wb") as f:
            np.save(f, sd)
    else:
        logger.info("is_train is False!!!!")
        with open(opj(cfg_base.output_dir, f"{cfg_base.task_name}_{cfg_base.model_name}_{cfg_base.exp_name}_mn.npy"), "rb") as f:
            mn = np.load(f)
        with open(opj(cfg_base.output_dir, f"{cfg_base.task_name}_{cfg_base.model_name}_{cfg_base.exp_name}_sd.npy"), "rb") as f:
            sd = np.load(f)

    x_data = (x_data - mn)/sd
    # y_data = (y_data - mn)/sd

    if is_train:
        logger.info(f"Average ratio =,{mn},and Average std =, {sd}")

    return x_data, y_data

# import calendar
# import numpy as np
# calendar.setfirstweekday(6)
# Ref : https://stackoverflow.com/questions/3806473/week-number-of-the-month
def get_week_of_month(year, month, day):
    x = np.array(calendar.monthcalendar(year, month))
    week_of_month = np.where(x==day)[0][0]
    return(week_of_month)


from sklearn.metrics import mean_squared_error

# np.sqrt(sum((y_true - y_pred)**2)  / len(y_pred))
def rmse(y_true, y_pred):
    # sklearn.metrics 패키지에는 별도의 RMSE 평가지표는 없습니다
    return np.sqrt(mean_squared_error(y_true, y_pred))


# Ref : https://www.kaggle.com/code/cdeotte/gru-model-3rd-place-gold?scriptVersionId=133950942&cellId=40
def smape(y_true, y_pred):

    # CONVERT TO NUMPY
    y_true = np.array(y_true.flatten())
    y_pred = np.array(y_pred.flatten())

    # WHEN BOTH EQUAL ZERO, METRIC IS ZERO
    both = np.abs(y_true) + np.abs(y_pred)
    idx = np.where(both==0)[0]
    y_true[idx]=1; y_pred[idx]=1

    return 100/len(y_true) * np.sum(2 * np.abs(y_pred - y_true) / (np.abs(y_true) + np.abs(y_pred)))