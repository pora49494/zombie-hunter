import sys
import configparser
from confluent_kafka import Consumer, KafkaError, TopicPartition, Producer
from confluent_kafka.admin import AdminClient, NewTopic

topic_header = sys.argv[1]
config = configparser.ConfigParser()
config.read('/app/config.ini')

# filter data
for i in range(int(config['BGPScheduler']['PartitionNumber'])) :
    year, month = map(int, topic_header.split("_")) 
    f = open(f"{config['ZombieDetector']['Result']}/{year}-{month}-zombieDetector-{i}.txt", 'r')
    fw = open(f"{config['ZombieDetector']['Result']}/{year}-{month}-zombieDetector-{i}-filtered.txt", 'w+')    
    
    line = f.readline()
    while line :
        deque = line.split('|')[-1]
        queue = deque.split("[")[1].split("]")[0]
        arr = list(map(int, queue.split(",")))
        maxlen = len(arr)
        check = False
        
        for i in range(maxlen) :
            if arr[-maxlen+i] <= arr[-maxlen]//2 :
                check = True 
            if arr[-maxlen+i] > arr[-maxlen]//2 and check : 
                check = False
                break

        if check and arr[0] >= arr[1] // 2 : 
            fw.write(f"{line}")
            
        line = f.readline()
    
    f.close()
    fw.close()

consumer = Consumer({ 
    'bootstrap.servers': config['DEFAULT']['KafkaServer'],
    'group.id': 'mygroup',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'},
})
admin_client = AdminClient({'bootstrap.servers': config["DEFAULT"]["KafkaServer"] })
clusterMetaData = consumer.list_topics()

# gather topic name 
topics_delete = []
for key in clusterMetaData.topics : 
    if topic_header in key :
        topics_delete.append(key)

# delete finished topics
admin_client.delete_topics(topics_delete)
