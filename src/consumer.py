from confluent_kafka import Consumer
import json
from src.producer import topic
from src.fraud_rules import is_type_in, check_rules
from src.redis_helper import add_transaction
from src.ml_helper import load_random_forest, load_xgboost
from src.data_processing import IN_OUT
import numpy as np 
import time 
from dotenv import load_dotenv
import os 
import warnings
warnings.filterwarnings('ignore')

load_dotenv()
TPS = int(os.getenv("TPS"))
TOTAL_TXN = TPS * 60
SAVE_NAME = os.getenv("SAVE_NAME")
conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "transactions-consumer",
    "auto.offset.reset": "earliest"
}

model = load_xgboost()
# model = load_random_forest()

def detect_fraud(tx: dict) -> bool:
    """ 
    Detect fraud by passing a transaction to rule-based system
    If rule based valids that it is not a fraud, pass to ML system 
    to double check the transaction
    return False if not a fraud
    """
    """
    if is_type_in(tx):
        return False
    """
    # return check_rules(tx)
    # if not check_rules(tx):
    #     """ 
    #     After rule based, we found that a transaction is not a fraud.
    #     So we pass it into ML to make sure we don't skip a fraud.
    #     """
    #     ml_txn = np.array([[val for key, val in tx.items() if key not in ('nameOrig', 'nameDest', 'isFraud', 'recv_time')]])
    #     # 3. Predict!
    #     return model.predict(ml_txn)[0] 
    
    ml_txn = np.array([[val for key, val in tx.items() if key not in ('nameOrig', 'nameDest', 'isFraud', 'recv_time')]])
    # 3. Predict!
    return model.predict(ml_txn)[0] 
    return True

def main():
    duration = []
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    count_TN = 0
    count_TP = 0
    count_FP = 0
    count_FN = 0
    try:
        while True:
            # constantly pulling new data from Kafka Server
            msg = consumer.poll(0.01)
            start_time = time.perf_counter()
            if msg is None:
                continue 
            if msg.error() is not None:
                print(f"Error: {msg.error()}")
                continue 
            # message OK without error 
            msg_value = msg.value().decode('utf-8')
            transaction = json.loads(msg_value)
            # print(transaction)
            is_fraud = detect_fraud(transaction)
            end_time = time.perf_counter()
            # duration.append(end_time - transaction['recv_time'])
            duration.append(end_time - start_time)
            if not is_fraud: 
                add_transaction(IN_OUT[transaction['type_CASH_IN']] + transaction['nameOrig'], transaction)
                if transaction['isFraud'] == 0:
                    count_TN += 1
                else: 
                    count_FN += 1
            else:
                if transaction['isFraud'] == 1:
                    count_TP += 1
                else: 
                    count_FP += 1
            if len(duration) >= TOTAL_TXN:
                break
            
    except KeyboardInterrupt: 
        print("Consumer shut down.")
    finally:
        with open(SAVE_NAME, 'w') as f: 
            if len(duration) > 0:
                duration = np.array(duration)
                # 3. Calculate your percentiles
                p50 = np.percentile(duration, 50)
                p95 = np.percentile(duration, 95)
                p99 = np.percentile(duration, 99)
                average_latency = np.mean(duration)
                f.write(f"TP fraud: {count_TP}\n")
                f.write(f"TN non fraud: {count_TN}\n")
                f.write(f"FP n-fraud: {count_FP}\n")
                f.write(f"FN fraud: {count_FN}\n")
                f.write(f"Average: {average_latency:.4f} seconds\n")
                f.write(f"p50: {p50:.4f} seconds\n")
                f.write(f"p95: {p95:.4f} seconds\n")
                f.write(f"p99: {p99:.4f} seconds\n")
                f.write(f"Total Duration: {sum(duration)}\n")
            else:
                print("Consumer shut down before processing any transactions. No metrics to display.\n")
        # print(f"Durations: {duration}")
        print("Consumer close")
        consumer.close()
        
if __name__ == "__main__":
    main()