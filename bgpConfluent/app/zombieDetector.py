import msgpack
import datetime
import random
import time
import logging
import argparse
import os
import arrow
import configparser
from collections import deque, defaultdict
from confluent_kafka import Consumer, KafkaError, TopicPartition 

def ts2dt(ts): 
    return datetime.datetime.fromtimestamp( ts + datetime.datetime(1970, 1, 1).timestamp() ) 

def dt2ts(dt): 
    return int((dt - datetime.datetime(1970, 1, 1)).total_seconds())

class ZombieDetector : 
    def __init__(self, partition, start, end ): 
        self.partition = partition
        self.start = start 
        self.end = end 
        
        self.config = configparser.ConfigParser()
        self.config.read('/app/config.ini')

        file_loc = f"{self.config['ZombieDetector']['Result']}/{start.year}-{start.month}-zombieDetector-{args.partition}.txt"    
        self.file = open(file_loc, "w+")

        FORMAT = '%(asctime)s ZombieDetector %(message)s'
        logging.basicConfig(
            format=FORMAT, filename=f'{self.config["DEFAULT"]["LogLocation"]}/{start.year}-{start.month}-ihr-kafka-ZombieDetector.log',
            level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S'
        )

        maxlen = 3600000 // int(self.config['DEFAULT']['Interval'])
        self.over_50 = defaultdict(lambda: True)
        self.max_peer = defaultdict(int)
        self.prefixes = defaultdict(lambda: deque(maxlen=maxlen+2))
        
    def get_consumer(self) :
        consumer = Consumer({ 
            'bootstrap.servers': self.config['DEFAULT']['KafkaServer'],
            'group.id': 'mygroup',
            'client.id': 'client-1',
            'enable.auto.commit': True,
            'session.timeout.ms': 6000,
            'default.topic.config': {'auto.offset.reset': 'smallest'},
        })

        topicPartitions = [ TopicPartition( f"{os.environ['TOPIC']}_{self.config['BGPScheduler']['SchedulerTopic']}", self.partition, dt2ts(self.start)*1000 ) ]
        offsetsTimestamp = consumer.offsets_for_times(topicPartitions)
        consumer.assign(offsetsTimestamp)        
        return consumer

    def run(self) :         
        name = f"zombieDetector-{self.partition}"
        logging.info(f"[{name}] start consuming")

        maxlen = 3600000 // int(self.config['DEFAULT']['Interval'])
        consumer = self.get_consumer()

        try :
            while True :
                msg = consumer.poll(1)
                if msg is None :
                    continue

                elif not msg.error() :
                    message = msgpack.unpackb(msg.value(), raw=False)
                    if 'end' in message :
                        if message['error'] :
                            logging.error(f"[{name}] exit with error")
                        break

                    timestamp = msg.timestamp()[1] 
                    self._process(message, timestamp, maxlen)

                elif msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.warning(f'[{name}] end of partition reached {msg.partition()}')
                else:
                    logging.error(f'[{name}] {msg.error().str()}')

        except KeyboardInterrupt :
            logging.info("recieve ctrl-c exit")
            return 
        
        except Exception as e :
            logging.error(f"[{name}] {e}") 
            return 
        
        finally : 
            consumer.close()
            logging.info(f'[{name}] done consuming')            
            self.file.close()

    def _process(self, status, ts, maxlen) :
        
        p = status['prefix']
        v = int(status['value'])

        self.prefixes[p].append(v)
        self.max_peer[p] = max(v, self.max_peer[p])

        if len( self.prefixes[p] ) < maxlen+2:
            return  
    
        if self.prefixes[p][-maxlen] < self.max_peer[p]*0.5 and self.prefixes[p][-maxlen-1] >= self.max_peer[p]*0.5 :
            if self.prefixes[p][-1] < self.max_peer[p]*0.5 and self.prefixes[p][-1]:
                content = f"zombieDetector-{self.partition} | {p} | {ts2dt(ts//1000)} | {self.max_peer[p]} | {self.prefixes[p]} \n"
                self.file.write(content)

if __name__ == '__main__':
    text = "This script look for the zombie activity in data recieved from the scheduler"

    parser = argparse.ArgumentParser(description=text)
    parser.add_argument("--partition", "-p", help="Choose topic partition you want to consume")
    parser.add_argument("--startTime", "-s", help="Choose start time (Format: Y-m-dTH:M:S; Example: 2017-11-06T16:00:00)")
    parser.add_argument("--endTime", "-e", help="Choose end time (Format: Y-m-dTH:M:S; Example: 2017-11-06T16:00:00)")
    args = parser.parse_args()

    start = arrow.get(args.startTime)
    end = arrow.get(args.endTime)

    assert start.hour % 8 == 0, "You must download rib file at 8:00am, 16:00pm or 24:00am"

    ZombieDetector(int(args.partition), start.naive, end.naive).run()
     