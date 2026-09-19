# Real-time fraud detection pipeline

## Overview 
This project is built for fraud detection simulation with pre-cleaned data. It focuses on the detection pipeline. Transactions are sent to Kafka and then processed with a hybrid method. The transactions are evaluated with rule-based and XGBoost. This significantly improves the correctness to flag a fraud transaction.

## Introduction
A hybrid method is used for fraud detection. Firstly, the rule-based part examines transactions with defined rules. If the transaction is safe, it is passed to the XGBoost model for a second examination to prevent the risk of losing money. Transactions detected as fraud by the rule-based part are sent back directly without passing through the machine learning part. 

The PaySim dataset is used in this project for simulation. It is a highly imbalanced dataset that contains only 0.13% of fraud transactions. Undersampling was applied for model training, but evaluation and test set still use the original dataset after data splitting. RandomForest and XGBoost were trained but only XGBoost is integrated into the pipeline for fraud detection. Users can integrate RandomForest by modifying the model integration function.

The data is processed for feature engineering to add more information before passing to the model. For example, the frequency of the transactions done by the same person and whether they’re done at night time. The dataset is split to 8:1:1 for training, evaluation, and testing respectively. The training dataset is then undersampled to 1:0.4 to avoid the model predicting everything as negative to get a high score. Then, the model is trained and tested with this dataset to find the best parameters. Data processing and model training are done in Jupyter notebooks.

Rules used in algorithmic checks are taken from the Alibaba Cloud (2025) website: "A Guide to Preventing Fraud Detection in Real-Time with Apache Flink". There are three main rules: 
- Small-to-Large Transfer Pattern,
- Money Mule Pattern,
- Pump-and-Dump Pattern

The model achieved 84% recall but only 24% precision as the model was trained with an undersampled dataset but tested with the original dataset. This approach tests the model's ability in production to avoid an unrealistic precision estimate. A trade-off was made to choose the percentage of undersampling by balancing the precision and recall rate. Since the most important indicator in fraud detection is the recall rate, it was prioritized during the trade-off.

## Set Up
Set up is already configured. You just need to execute the command below to set up Kafka and Redis.
```
make all
```

To start only one service, execute
```
make up-kafka
```
or 
```
make up-redis
```

To stop them, execute
```
make down-all
```

To start the pipeline, launch the consumer with the command below. It will then start processing transactions when Kafka receives one.
```
python -m src.consumer
```

This project contains only the fraud detector and users need to connect themselves. To simplify this step, transactions for testing are prepared and put in the data folder. To simulate transactions, launch the producer with the command below.
```
python -m src.producer
```

## Contributions
This is an individual project.

## License
This project is licensed under the GNU General Public License v3.0. See the LICENSE file for full details.