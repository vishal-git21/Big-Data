import sys
import json
from kafka import KafkaConsumer


if len(sys.argv) < 4:
    print("Usage: python kafka-consumer3.py <topicName1> <topicName2> <topicName3>")
    sys.exit(1)


topic_name3 = sys.argv[3]


user_popularity = {}


consumer3 = KafkaConsumer(topic_name3, bootstrap_servers='localhost:9092', value_deserializer=lambda v: v.decode('utf-8'))


for message in consumer3:
    if message.value == "EOF":
        break 
    parts = message.value.split()
    if len(parts) >= 4:
        action = parts[0]
        user = parts[2]
        target_user = parts[1]
        post_id = parts[3]
        comments = 0
        shares = 0
        likes = 0
        if action == 'share':
            shares = len(parts) - 4
        if action == 'like':
            likes = 1
        if action == 'comment':
            comments = len(parts[4].split(' '))

        
        popularity = (likes + 20*shares + 5*comments) / 1000
        
        if user in user_popularity:
            user_popularity[user] += popularity
        else:
            user_popularity[user] = popularity
            
rounded_popularity_dict = {user: round(popularity, 3) for user, popularity in sorted(user_popularity.items())}


print(json.dumps(rounded_popularity_dict, indent=4))

