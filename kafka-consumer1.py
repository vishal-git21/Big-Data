import sys
import json
from kafka import KafkaConsumer


if len(sys.argv) < 2:
    print("Usage: python kafka-consumer1.py <topicName1> [topicName2] [topicName3]")
    sys.exit(1)


topic_name1 = sys.argv[1]+'a'


user_comments = {}


consumer1 = KafkaConsumer(topic_name1, bootstrap_servers='localhost:9092', value_deserializer=lambda v: v.decode('utf-8'))


for message in consumer1:
    if message.value == "EOF":
        break  
    parts = message.value.split()
    if len(parts) >= 5:
        user, comment = parts[2], " ".join(parts[4:]).strip('\"')  
        user_comments.setdefault(user, []).append(comment)


sorted_user_comments = dict(sorted(user_comments.items(), key=lambda item: item[0]))


print(json.dumps(sorted_user_comments, indent=4))

