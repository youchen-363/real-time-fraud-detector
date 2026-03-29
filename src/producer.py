from confluent_kafka import Producer 
import csv, time, json

conf = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "paysim-data-generator"
}

topic = "raw-transactions"
file_name = "data/data.csv"

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush()."""
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    # else:
    #     print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
    
def tps(reader):
    # TODO: Integrate into the producer to test diff TPS
    
    # --- ADD THIS BEFORE YOUR LOOP ---
    target_tps = 1000  # Change this to 100, 1000, 10000, or 0 for "Unlimited"
    start_time = time.time()

    for count, row in enumerate(reader, start=1): # Start counting at 1 to avoid DivisionByZero
        pass
    # ... [Your JSON dumps, routing_key, produce(), and poll(0) go here] ...
    
    # --- ADD THE PACER LOGIC AT THE END OF THE LOOP ---
    if target_tps > 0:
        expected_time = count / target_tps
        actual_time = time.time() - start_time
        sleep_time = expected_time - actual_time
        
        # If we are running faster than the target TPS, pause briefly
        if sleep_time > 0:
            time.sleep(sleep_time)
            
    # Print an update every 10,000 rows so you can verify the speed visually
    if count % 10000 == 0:
        print(f"Sent {count} transactions... Current target: {target_tps} TPS")    

def main():       
    producer = Producer(conf)
    with open(file_name, mode='r') as file:
        csv_file = csv.DictReader(file)
        for idx, row in enumerate(csv_file):
            data = json.dumps(row)
            routing_key = row["nameOrig"]
            producer.produce(
                topic=topic,
                key=routing_key,
                value=data,
                callback=delivery_report
            )
            producer.poll(0)
    producer.flush()

if __name__ == "__main__":
    main()