import json
import time
from kafka import KafkaProducer

events = [
    {"user_id": "123", "event_type": "click", "timestamp": "2023-10-27T10:00:00Z", "game_id": "456", "payload": {"button_id": "789"}},
    {"user_id": "124", "event_type": "login", "timestamp": "2023-10-27T10:01:00Z", "game_id": "457", "payload": {}}
]

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for event in events:
    producer.send('user_events', event)
    time.sleep(1)
