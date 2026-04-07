import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
        bootstrap_servers= ['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open('grocery.json','r') as f:
    grocery_list = json.load(f)

print("Strating the streaming data to Kafka")

for item in grocery_list:
    producer.send('grocery-topic', value=item )
    print(f"Sent to Kafka: {item.get('item_name', 'Unknown Item')}")
    time.sleep(2)

producer.flush()
producer.close()
print("All data Sent")