# https://www.tensorflow.org/api_docs/python/tf/keras/layers/GRU  (docs)
# Ref : https://www.kaggle.com/code/cdeotte/gru-model-3rd-place-gold?scriptVersionId=133950942&cellId=35
import tensorflow as tf
from tensorflow.keras import layers
def build_rnn_model(cfg):

    inp = tf.keras.Input(shape=(cfg.data.lookback_window,len(cfg.data.feature_list))) # INPUT SHAPE IS

    assert cfg.base.model_name in ['GRU', 'LSTM'], "check model_name"

    RNN = getattr(layers, cfg.base.model_name)
    x = RNN(units=64, return_sequences=False)(inp)
    # x = RNN(units=64, return_sequences=True)(inp)
    # x = RNN(units=32, return_sequences=True)(x)
    # x = RNN(units=8, return_sequences=False)(x)
    x = layers.Dense(1,activation='linear')(x) # OUTPUT SHAPE IS 5
    model = tf.keras.Model(inputs=inp, outputs=x)

    opt = tf.keras.optimizers.Adam(learning_rate=1e-3)
    loss = tf.keras.losses.MeanSquaredError()
    model.compile(loss=loss, optimizer = opt)

    return model