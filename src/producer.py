from confluent_kafka import Producer 
import time, json
import pyarrow.parquet as pq
from dotenv import load_dotenv
import os  

load_dotenv()
FILE_NAME = os.getenv("FILE_NAME")
TARGET_TPS = int(os.getenv("TPS"))

conf = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "paysim-data-generator"
}

topic = "raw-transactions"

# try:
#     TARGET_TPS = int(sys.argv[-1])
# except (IndexError, ValueError):
#     TARGET_TPS = 100  # Default fallback

TEST_DURATION = 60
MAX_MESSAGES = TARGET_TPS * TEST_DURATION

SLEEP_TIME = 1.0 / TARGET_TPS
    
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush()."""
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    # else:
    #     print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
            
def main():       
    count = 0
    producer = Producer(conf)
    parquet_file = pq.ParquetFile(FILE_NAME)
    start_time = time.time()
    
    # 2. Read the file in chunks of 5,000 rows at a time
    # This guarantees your PC will never run out of RAM!
    for batch in parquet_file.iter_batches(batch_size=5000):
        
        # Convert this specific chunk into a list of dictionaries
        records = batch.to_pylist()
        
        for row in records:            
            # PyArrow might read numbers as special types, so we ensure 
            # it converts cleanly to a JSON string
            row["recv_time"] = time.perf_counter()
            data = json.dumps(row)
            # print(data)
            routing_key = str(row["nameOrig"])
            # print("\n\n", routing_key)
            
            while True:
                try:
                    producer.produce(
                        topic=topic,
                        key=routing_key,
                        value=data,
                        callback=delivery_report
                    )
                    break # Success! Break the while loop
                except BufferError:
                    # Queue is full! Wait 0.1 seconds and try again
                    producer.poll(0.1)
            count += 1
            
            if count >= MAX_MESSAGES:
                print(f"\nTarget volume of {MAX_MESSAGES} reached. Stopping Producer.")
                producer.flush()
                return  # Instantly exits the main() function!
            
            # --- ADD THE PACER LOGIC AT THE END OF THE LOOP ---
            if TARGET_TPS > 0:
                expected_time = count / TARGET_TPS
                actual_time = time.time() - start_time
                sleep_time = expected_time - actual_time
                
                # If we are running faster than the target TPS, pause briefly
                if sleep_time > 0:
                    time.sleep(sleep_time)
                
            # --- VISUAL VERIFICATION FOR YOUR DEMO ---
            if count % TARGET_TPS == 0:
                elapsed = time.time() - start_time
                real_tps = count / elapsed
                print(f"Sent {count:,} transactions... Current Speed: {real_tps:.0f} TPS")
                
    producer.flush()

if __name__ == "__main__":
    main()