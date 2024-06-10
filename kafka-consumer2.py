import sys
import json
from kafka import KafkaConsumer


if len(sys.argv) < 2:
    print("Usage: python kafka-consumer2.py <topicName2>")
    sys.exit(1)


topic_name2 = sys.argv[2]+'b'


user_likes = {}


consumer2 = KafkaConsumer(topic_name2, bootstrap_servers='localhost:9092', value_deserializer=lambda v: v.decode('utf-8'))


for message in consumer2:
    if message.value == "EOF":
        break  
    parts = message.value.split()
    if len(parts) >= 4:
        user = parts[2]
        post_id = parts[3]

        
        user_likes.setdefault(user, {}).setdefault(post_id, 0)
        user_likes[user][post_id] += 1


sorted_user_likes = dict(sorted(user_likes.items(), key=lambda item: item[0]))


print(json.dumps(sorted_user_likes, indent=4))

