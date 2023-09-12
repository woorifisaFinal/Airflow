
export stage_dir=/opt/airflow
export config_dir=/opt/airflow/stage1/config
pwd
cd ${stage_dir}
echo ${stage_dir}
python -m stage1.tools.infer -C ${config_dir}/tw/kor3y_lstm_infer.yml -D 20220414
python -m stage1.tools.infer -C ${config_dir}/tw/kor10y_lstm_infer.yml -D 20220414
python -m stage1.tools.infer -C ${config_dir}/tw/us10y_lstm_infer.yml -D 20220414
python -m stage1.tools.infer -C ${config_dir}/tw/us3y_lstm_infer.yml -D 20220414
python -m stage1.tools.infer -C ${config_dir}/sm/ftse_lstm_infer.yml -D 20220414
python -m stage1.tools.infer -C ${config_dir}/sm/nasdaq_lstm_infer.yml -D 20220414
python -m stage1.tools.infer -C ${config_dir}/sm/nikkei_lstm_infer.yml -D 20220414
python -m stage1.tools.infer -C ${config_dir}/jh/brazil_lstm_infer.yml -D 20220414
python -m stage1.tools.infer -C ${config_dir}/jh/india_lstm_infer.yml -D 20220414
python -m stage1.tools.infer -C ${config_dir}/jh/taiwan_lstm_infer.yml -D 20220414
python -m stage1.tools.infer -C ${config_dir}/jw/ks_lstm_infer.yml -D 20220414
python -m stage1.tools.infer -C ${config_dir}/bg/gold_lstm_infer.yml -D 20220414
python -m stage1.output.output_collection -D 20220414
