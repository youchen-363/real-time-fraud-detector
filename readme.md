# Real-time fraud detection pipeline

## Overview 
This project is built for fraud detection simulation with pre-cleaned data. It focuses on the detection pipeline. Transactions are sent to kafka and then processed with a hybrid method. The transactions are evaluated with rule-based and XGBoost. This highly increased the correctness to flag a fraud transaction.

## Introduction
Hybrid method is used for fraud detection. Firstly, rule-based examines transactions with defined rules. If the transaction is safe, it is passed to XGBoost model for second examination to prevent the risk of losing money. Transactions detected as fraud by rule-based part are sent back directly without passing through the machine learning part. 

contains notebooks used for training XGBoost model. 

## Set Up

Set up is already configured. You just need to execute the command below to set up kafka and redis.
'''
make all
'''

To stop them, execute
'''
make down-all
'''

To start the pipeline, launch the consumer with the command below. It will then start processing transactions when kafka receives one.
'''
python -m src.consumer
'''

This project contains only the fraud detector, users need to connect themselves. To simplify this step, transactions for testing are prepared and put in the data folder. To simulate transactions, launch producer with the command below.
'''
python -m src.producer
'''