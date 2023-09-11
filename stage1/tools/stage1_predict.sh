#!/usr/bin/env bash

### It should be executed in week3 director (i.e. sup dir of stage1)

config_dir = /opt/airflow/stage1/config

################################### tw ###################################

# kor3y

python -m stage1.tools.infer -C {config_dir}\tw\kor3y_lstm_infer.yml

# kor10y

python -m stage1.tools.infer -C {config_dir}\tw\kor10y_lstm_infer.yml

# us10y

python -m stage1.tools.infer -C {config_dir}\tw\us10y_lstm_infer.yml

# us3y

python -m stage1.tools.infer -C {config_dir}\tw\us3y_lstm_infer.yml

################################### sm ###################################

# uk

python -m stage1.tools.infer -C {config_dir}\sm\ftse_lstm_infer.yml

# us

python -m stage1.tools.infer -C {config_dir}\sm\nasdaq_lstm_infer.yml

# jp

python -m stage1.tools.infer -C {config_dir}\sm\nikkei_lstm_infer.yml

################################### jh ###################################
# br

python -m stage1.tools.infer -C {config_dir}\jh\brazil_lstm_infer.yml

# ind

python -m stage1.tools.infer -C {config_dir}\jh\india_lstm_infer.yml

# tw (taiwan)

python -m stage1.tools.infer -C {config_dir}\jh\taiwan_lstm_infer.yml

################################### jw ###################################

# kor

python -m stage1.tools.infer -C {config_dir}\jw\ks_lstm_infer.yml

################################### hs ###################################

# euro

# python -m stage1.tools.infer -C {config_dir}\hs\euro_lstm_infer.yml

################################### bg ###################################
# gold

python -m stage1.tools.infer -C {config_dir}\bg\gold_lstm_infer.yml

####### 최종 수집
python -m stage1.output.output_collection
