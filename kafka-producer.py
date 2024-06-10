import sys
from kafka import KafkaProducer


if len(sys.argv) < 4:
    print("Usage: python kafka-producer.py <topicName1> <topicName2> <topicName3>")
    sys.exit(1)


topic_name1, topic_name2, topic_name3 = sys.argv[1:4]


producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: v.encode('utf-8'))


for line in sys.stdin:
    line = line.strip()
   
    if not line:
        continue
    
    
    if line != "EOF":
        
        words = line.split()
        if len(words) >= 4:
            action = words[0]

            if action == 'comment':
             
                producer.send(topic_name1, value=line)
            elif action == 'like':
                
                producer.send(topic_name2, value=line)

            
            producer.send(topic_name3, value=line)


producer.send(topic_name1,value = "EOF")
producer.send(topic_name2,value = "EOF")
producer.send(topic_name3,value = "EOF")

producer.close()
