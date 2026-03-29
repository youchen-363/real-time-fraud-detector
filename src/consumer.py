from confluent_kafka import Consumer
import json
from producer import topic

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "transactions-consumer",
    "auto.offset.reset": "earliest"
}

def main():
    
    consumer = Consumer(conf)
    consumer.subscribe([topic])

    try:
        while True:
            # constantly pulling new data from Kafka Server
            msg = consumer.poll(1.0)
            if msg is None:
                continue 
            if msg.error() is not None:
                print(f"Error: {msg.error()}")
                continue 
            # message OK without error 
            msg_value = msg.value().decode('utf-8')
            transaction = json.loads(msg_value)
            print(transaction['nameOrig'])    
    except KeyboardInterrupt: 
        print("Consumer shut down.")
    finally:
        consumer.close()
        
if __name__ == "__main__":
    main()