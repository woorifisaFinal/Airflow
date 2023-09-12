

"""#LSTM"""
from os.path import join as opj
import numpy as np
import pandas as pd
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from tensorflow.keras.optimizers import Adam
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import joblib
__all__ = ['gold_lstm']

def gold_lstm(cfg):
    data_nomalized = pd.read_csv(opj(cfg.base.data_dir, 'all_eda_data.csv'))
    variation_column = data_nomalized.pop('변동 %_gold')
    data_nomalized.insert(1, '변동 %_gold', variation_column)  # Insert 'Variation' column at the beginning

    if cfg.base.mode=='infer':

        if (data_nomalized['날짜']==cfg.base.base_date).sum()==0:
            # 존재하지 않는 날. 휴장
            pd.DataFrame(data={"date":cfg.base.base_date, cfg.base.task_name:np.NaN},index=[0]).to_csv(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_{cfg.base.base_date}.csv"), index=False)
            return 
        test = data_nomalized[data_nomalized['날짜']<cfg.base.base_date].tail(50)
        test = test.reset_index(drop=True)

        first_data = test
        stock_data = first_data.drop(columns=['날짜'])

        stock_data_변동 = stock_data['변동 %_gold']

        stock_data_test = stock_data.drop(columns=['변동 %_gold'])
    
        scaler = joblib.load(opj(cfg.base.output_dir,'gold_scaler.pkl'))

        stock_data_scaled_test = scaler.transform(stock_data_test)

        # stock_data_test와 stock_data_2는 같아 보여서 stock_data_2를 stock_data_test로 수정했음 (columns 부분)
        stock_data_scaled = pd.DataFrame(data=stock_data_scaled_test, columns=stock_data_test.columns)

  
        stock_data_scaled = stock_data_scaled.reset_index(drop=True)

        stock_data_scaled = pd.concat([stock_data_변동, stock_data_scaled], axis=1)

        """***61 거래량_WTI 1535 non-null float64 여기에 있는 null 때문에 생긴 문제입니다.***"""

        stock_data_scaled = stock_data_scaled.values

        testX = []
        testX.append(stock_data_scaled)
        testX = np.array(testX)

    elif cfg.base.mode=='train':
        train = data_nomalized[data_nomalized['날짜'].between('2017-01-01', '2020-12-31')]
        vaildation = data_nomalized[data_nomalized['날짜'].between('2021-01-01', '2021-12-31')]
        test = data_nomalized[data_nomalized['날짜'].between('2022-01-01', '2022-12-31')]

        train = train.reset_index(drop=True)
        vaildation = vaildation.reset_index(drop=True)
        test = test.reset_index(drop=True)

        first_data = data_nomalized

        first_data.columns.nunique()

        #오리지날 수익률 저장하기
        original_volatility = first_data['변동 %_gold'].values

        #오리지날 날짜 저장하기
        dates = pd.to_datetime(first_data['날짜'])

        #날짜 제외하고 만들기
        stock_data = first_data.drop(columns=['날짜'])

        stock_data_변동 = stock_data['변동 %_gold']

        stock_data_2 = stock_data.drop(columns=['변동 %_gold'])

        stock_data_train_val=stock_data_2[0:len(train)+len(vaildation)]

        stock_data_test = stock_data_2[len(train)+len(vaildation):]

        # normalize the dataset- x
        scaler = StandardScaler()
        scaler = scaler.fit(stock_data_train_val)
        
        joblib.dump(scaler, opj(cfg.base.output_dir, 'gold_scaler.pkl'))    

        stock_data_scaled = scaler.transform(stock_data_train_val) #train
        stock_data_scaled_test = scaler.transform(stock_data_test)

        stock_data_scaled = pd.DataFrame(data=stock_data_scaled, columns=stock_data_2.columns)

        stock_data_scaled_test = pd.DataFrame(data=stock_data_scaled_test, columns=stock_data_2.columns)

        stock_data_scaled = pd.concat([stock_data_scaled, stock_data_scaled_test], axis=0)

        stock_data_scaled = stock_data_scaled.reset_index(drop=True)

        stock_data_scaled = pd.concat([stock_data_변동, stock_data_scaled], axis=1)

        """***61 거래량_WTI 1535 non-null float64 여기에 있는 null 때문에 생긴 문제입니다.***"""

        stock_data_scaled = stock_data_scaled.values



        # split to train data and test data
        n_train = len(train)
        train_data_scaled = stock_data_scaled[0: n_train]
        train_dates = dates[0: n_train]

        n_validation = len(train) + len(vaildation)
        val_data_scaled = stock_data_scaled[n_train:n_validation]
        val_dates = dates[n_train:n_validation]

        n_test = n_validation
        test_data_scaled = stock_data_scaled[n_test:]
        test_dates = dates[n_test:]

        # data reformatting for LSTM
        pred_days = 30  # prediction period
        seq_len = 50   # sequence length = past days for future prediction.
        input_dim = 74  # input_dimension = ['Open', 'High', 'Low', 'Close', 'Volume']

        trainX = []
        trainY = []
        valX = []
        valY = []
        testX = []
        testY = []

        # 추론 날짜 (base_date 출력을 위한)
        val_dates_for_infer = []
        test_dates_for_infer = []
        # val_dates, test_dates는 pd.Series로 되어 있고 numpy datetime으로 되어 있다.
        val_dates = val_dates.astype('string')
        test_dates = test_dates.astype('string')

        for i in range(seq_len, n_train-pred_days +1):
            trainX.append(train_data_scaled[i - seq_len:i, 0:train_data_scaled.shape[1]])
            trainY.append(train_data_scaled[i + pred_days - 1:i + pred_days, 0])
        
        for i in range(seq_len, len(val_data_scaled)-pred_days +1):
            valX.append(val_data_scaled[i - seq_len:i, 0:val_data_scaled.shape[1]])
            valY.append(val_data_scaled[i + pred_days - 1:i + pred_days, 0])
            # base_dates추가
            val_dates_for_infer.append(val_dates.iloc[i])
        
        for i in range(seq_len, len(test_data_scaled)-pred_days +1):
            testX.append(test_data_scaled[i - seq_len:i, 0:test_data_scaled.shape[1]])
            testY.append(test_data_scaled[i + pred_days - 1:i + pred_days, 0])
            # base_dates추가
            test_dates_for_infer.append(test_dates.iloc[i])

        trainX, trainY = np.array(trainX), np.array(trainY)
        # print("trainX.shape : ", trainX.shape)
        # print("trainY.shape : ", trainY.shape)
        valX, valY = np.array(valX), np.array(valY)
        testX, testY = np.array(testX), np.array(testY)

    from tensorflow.keras.initializers import Constant
    from tensorflow.keras.regularizers import l2
    from tensorflow.keras.constraints import MinMaxNorm
    from keras.callbacks import EarlyStopping, ModelCheckpoint
    from keras.layers import Dropout
    from keras.layers import LSTM, Dense, BatchNormalization

    from keras.optimizers import SGD, RMSprop, Adam, Adadelta, Adagrad, Nadam

    # LSTM model
    model = Sequential()
    # model.add(LSTM(64, input_shape=(trainX.shape[1], trainX.shape[2]), # (seq length, input dimension)
    #                return_sequences=True))
    # model.add(LSTM(32, return_sequences=False))

    # model.add(Dense(trainY.shape[1]))

    # LSTM model
    model = Sequential()
    # model.add(LSTM(64, input_shape=(trainX.shape[1], trainX.shape[2]), # (seq length, input dimension)
    #             return_sequences=True))
    model.add(LSTM(64, input_shape=(50, 74), # (seq length, input dimension)
                return_sequences=True))
    model.add(BatchNormalization())  # Add BatchNormalization here

    model.add(LSTM(32, return_sequences=True))
    model.add(BatchNormalization())
    model.add(LSTM(16, return_sequences=False))

    model.add(BatchNormalization())
    # model.add(Dense(trainY.shape[1]))
    model.add(Dense(1))

    # model.summary()


    if cfg.base.mode=='train':
        # specify your learning rate
        learning_rate = 0.003
        # create an Adam optimizer with the specified learning rate
        optimizer = Adam(learning_rate=learning_rate)
        # compile your model using the custom optimizer
        model.compile(optimizer=optimizer, loss='mse') #이거 mae로 바꿈

        # # Try to load weights
        # try:
        #     model.load_weights('lstm_weights.h5')
        #     print("Loaded model weights from disk")
        # except:
        #     print("No weights found, training model from scratch")
        #     # Fit the model
        history = model.fit(trainX, trainY, epochs=100, batch_size=16,
                        validation_data=(valX, valY), verbose=1)
        # Save model weights after training
        model.save_weights(opj(cfg.base.output_dir, 'lstm_weights.h5'))

        # plt.plot(history.history['loss'], label='Training loss')
        # plt.plot(history.history['val_loss'], label='Validation loss')
        # plt.legend()
        # plt.show()

    elif cfg.base.mode=='valid':
        model.load_weights(opj(cfg.base.output_dir, 'lstm_weights.h5'))

        val_pred = model.predict(valX)
        test_pred = model.predict(testX)

        import pickle
        with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_21.pkl"), 'wb') as f:
            pickle.dump(val_pred.reshape(-1,), f)

        with open(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_22.pkl"), 'wb') as f:
            pickle.dump(test_pred.reshape(-1,), f)
            
        # prediction = model.predict(testX)
        # print(prediction.shape, testY.shape)
        # 결과 저장
        pd.DataFrame(data={"date":val_dates_for_infer, cfg.base.task_name:val_pred.reshape(-1,)}).to_csv(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_21.csv"), index=False)

        pd.DataFrame(data={"date":test_dates_for_infer, cfg.base.task_name:test_pred.reshape(-1,)}).to_csv(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_22.csv"), index=False)
    else:
        model.load_weights(opj(cfg.base.output_dir, 'lstm_weights.h5'))

        test_pred = model.predict(testX)
        pd.DataFrame(data={"date":cfg.base.base_date, cfg.base.task_name:test_pred.reshape(-1,)}).to_csv(opj(cfg.base.output_dir, f"{cfg.base.task_name}_prediction_{cfg.base.base_date}.csv"), index=False)
    
    # # prediction

    # prediction = model.predict(testX)
    # # prediction = model.predict(testX)
    # print(prediction.shape, testY.shape)

    # # generate array filled with means for prediction
    # mean_values_pred = np.repeat(scaler.mean_[np.newaxis, :], prediction.shape[0], axis=0)

    # # substitute predictions into the first column
    # mean_values_pred[:, 0] = np.squeeze(prediction)

    # # inverse transform
    # # y_pred = scaler.inverse_transform(mean_values_pred)[:,0]
    # y_pred = mean_values_pred[:,0]
    # print(y_pred.shape)

    # # generate array filled with means for testY
    # mean_values_testY = np.repeat(scaler.mean_[np.newaxis, :], testY.shape[0], axis=0)

    # # substitute testY into the first column
    # mean_values_testY[:, 0] = np.squeeze(testY)

    # # inverse transform
    # # testY_original = scaler.inverse_transform(mean_values_testY)[:,0]
    # testY_original = mean_values_testY[:,0]
    # print(testY_original.shape)

    # # # plotting
    # # plt.figure(figsize=(14, 5))

    # # # plot original 'Open' prices
    # # plt.plot(dates, original_volatility, color='green', label='Original volatility')

    # # # plot actual vs predicted
    # # plt.plot(test_dates[seq_len:], testY_original, color='blue', label='Actual volatility')
    # # plt.plot(test_dates[seq_len:], y_pred, color='red', linestyle='--', label='Predicted volatility')
    # # plt.xlabel('Date')
    # # plt.ylabel('volatility')
    # # plt.title('Original, Actual and Predicted volatility')
    # # plt.legend()
    # # plt.show()

    # # # Calculate the start and end indices for the zoomed plot
    # # zoom_start = len(test_dates) - 50
    # # zoom_end = len(test_dates)

    # # # Create the zoomed plot
    # # plt.figure(figsize=(14, 5))

    # # # Adjust the start index for the testY_original and y_pred arrays
    # # adjusted_start = zoom_start - seq_len

    # # plt.plot(test_dates[zoom_start:zoom_end],
    # #           testY_original[adjusted_start:zoom_end - zoom_start + adjusted_start],
    # #          color='blue',
    # #          label='Actual Open Price')

    # # plt.plot(test_dates[zoom_start:zoom_end],
    # #          y_pred[adjusted_start:zoom_end - zoom_start + adjusted_start ],
    # #          color='red',
    # #          linestyle='--',
    # #          label='Predicted Open Price')

    # # plt.xlabel('Date')
    # # plt.ylabel('Open Price')
    # # plt.title('Zoomed In Actual vs Predicted Open Price')
    # # plt.legend()
    # # plt.show()

    # loss = model.evaluate(testX, testY)
    # print("Test loss:", loss)

    # from sklearn.metrics import mean_absolute_error
    # mae = mean_absolute_error(testY_original, y_pred)
    # print("Mean Absolute Error:", mae)

    # model.save('Gold_model.h5')

