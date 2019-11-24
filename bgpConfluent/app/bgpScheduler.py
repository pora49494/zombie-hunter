import msgpack
import datetime
import arrow
import argparse
import logging
import configparser
from collections import defaultdict 
from confluent_kafka import Consumer, KafkaError, TopicPartition, Producer
from confluent_kafka.admin import AdminClient, NewTopic

def dt2ts(dt): 
    return int((dt - datetime.datetime(1970, 1, 1)).total_seconds())

def ts2dt(ts): 
    return datetime.datetime.fromtimestamp( ts + datetime.datetime(1970, 1, 1).timestamp() ) 

class BGPScheduler : 
    ''' BGPScheduler consume data from bgpSummarizer. It collect data from
    all collector and divide data into 10 different batch. It send data to 
    every batch once every minute'''
    def __init__(self, collectors, start, end, topic_header) :
        self.collectors = collectors
        self.start = start 
        self.end = end
        self.topic_header = topic_header

        self.prefixes = defaultdict(int) 
        
        self.config = configparser.ConfigParser()
        self.config.read('/app/config.ini')

        FORMAT = '%(asctime)s BGPScheduler %(message)s'
        logging.basicConfig(
            format=FORMAT, filename=f'{self.config["DEFAULT"]["LogLocation"]}/{start.year}-{start.month}-ihr-kafka-BGPScheduler.log',
            level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S'
        )

        self.producer = Producer({
            'bootstrap.servers': self.config["DEFAULT"]["KafkaServer"],
            'default.topic.config': {
                'compression.codec': 'snappy',
                'queue.buffering.max.messages': 1000000,        
            },
            'batch.num.messages': 10000*10,
            'message.max.bytes': 1000000*10,
            'linger.ms': 20.0
        })

        # self.buf = set()

    def _create_topic(self) :
        ''' create topic for bgpScheduler '''
        topicName = f"{self.topic_header}_{self.config['BGPScheduler']['SchedulerTopic']}"
        batchNum = int(self.config['BGPScheduler']['PartitionNumber'])
        
        logging.info(f"[{topicName}] try to create topic")        
        
        admin_client = AdminClient({'bootstrap.servers': self.config["DEFAULT"]["KafkaServer"] })
        topic_list = [NewTopic(topicName, num_partitions=batchNum, replication_factor=1)]
        created_topic = admin_client.create_topics(topic_list)    

        for topic, f in created_topic.items():
            try:
                f.result()  # The result itself is None
                logging.info(f"[{topic}] topic created")
                  
            except Exception as e:
                logging.error(f"failed to create topic {topic}: {e}")
        
        return topicName
        
    def get_consumer(self, topics): 
        willtry = 0
        while True :
            if willtry > 1000 :
                logging.error(f"failed to create consumer: no try left")
                return None
            try :
                ''' get_customer() create a consumer interface and set the offset to the 
                start timestamp. '''
                consumer = Consumer({ 
                    'bootstrap.servers': 'localhost:9092',
                    'group.id': 'mygroup',
                    'client.id': 'client-1',
                    'enable.auto.commit': True,
                    'session.timeout.ms': 6000,
                    'max.poll.interval.ms': 6000000,
                    'default.topic.config': {'auto.offset.reset': 'smallest'},
                }) 
                topicPartitions = [ TopicPartition(topic, 0, dt2ts(self.start)*1000 ) for topic in topics ]
                offsetsTimestamp = consumer.offsets_for_times(topicPartitions)
                consumer.assign(offsetsTimestamp)        
                logging.debug(f"[get_consumer] successfully create customer")

                return consumer
            
            except Exception as e:
                logging.error(f"failed to create consumer: {e}, try {willtry}/1000")
                
            willtry += 1

    def run(self) :
        ''' bgpSummarizer consuming rib and update, summarize it then publish. '''
        _ = self._create_topic()

        logging.info("[BGPScheduler] start comsuming summary")

        topics = [f"{self.topic_header}_ihr_bgp_summary_{c}" for c in self.collectors]  
        topicPartitionList = [ TopicPartition(t, 0) for t in topics ]
        topics_num = len(topics)
        
        consumer = self.get_consumer(topics)
        interval = int(self.config['DEFAULT']['Interval'])
        current = dt2ts(self.start)*1000 + interval

        if consumer is None :
            logging.error("No consumer found: exit")
            return 

        pause = 0
        try:
            while True:
                if pause >= topics_num :
                    self._produce(current)
                    consumer.resume(topicPartitionList)

                    if current >= dt2ts(self.end) * 1000 :
                        logging.info(f"[BGPScheduler] Produced final message at {self.end}")
                        self._send_end(False)
                        break 

                    self.prefixes.clear()  
                    current += interval         

                    pause = 0
                    
                    # if current in self.buf :
                    #     self.buf.remove(current)
                    #     logging.warning("[BGPScheduler] Found the buffer, start reading")
                    #     f = open( f'{self.config["BGPScheduler"]["BufLocation"]}/{self.topic_header}-{current}', "rb" )
                    #     for msg in f.read().strip().split(b'\n') :
                    #         message = msgpack.unpackb(msg, raw=False)
                    #         if 'end' in message :
                    #             logging.error("found premature end message, pausing the consumption")
                    #             consumer.pause([TopicPartition(message['topic'], 0)]) 
                    #             pause += 1
                    #         else :
                    #             prefix = message['prefix']
                    #             self.prefixes[prefix] += message['value'] 
                    
                msg = consumer.poll(0.1)
                if msg is None:
                    continue

                elif not msg.error(): 
                    msg_ts = msg.timestamp()[1]
                    if msg_ts < current :
                        logging.warning(f"{msg.topic()} timestamp not match, expected: {ts2dt(current//1000)} recieved: {ts2dt(msg_ts//1000)} diff : {msg_ts-current} action : drop")
                    # elif msg_ts > current : 
                    #     if msg_ts not in self.buf :
                    #         self.buf.add(msg_ts)
                    #     logging.warning(f"{msg.topic()} timestamp not match, expected: {ts2dt(current//1000)} recieved: {ts2dt(msg_ts//1000)} diff : {msg_ts-current} action : buffering")
                    #     f = open( f'{self.config["BGPScheduler"]["BufLocation"]}/{self.topic_header}-{msg_ts}', "wb+" ) 
                    #     f.write( msg.value() + b'\n' ) 
                    #     f.close()
                    #     continue

                    message = msgpack.unpackb(msg.value(), raw=False)
                    
                    if 'end' in message :
                        consumer.pause([TopicPartition(msg.topic(), 0)]) 
                        pause += 1
                    else :
                        prefix = message['prefix']
                        self.prefixes[prefix] += message['value']
                    
                elif msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.info(f'end of partition reached {msg.partition()}')
                else:
                    logging.error(f'error : {msg.error().str()}')
                     
        except KeyboardInterrupt:
            logging.info("recieve ctrl-c exit")
            return
        except Exception as e :
            logging.error(f"[BGPScheduler] exit with error : {e}")
            self._send_end(True)
            return
        
        finally:
            consumer.close()
            logging.info("[BGPScheduler] done comsuming summary")

    def _delivery_report(self, err, msg):
        ''' Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). '''
        if err is not None:
            logging.debug(f'message delivery failed: {err}')
        else:
            pass

    def _produce(self, ts) :
        for p in self.prefixes:
                
            while True:
                try:
                    self.producer.produce(
                        f"{self.topic_header}_{self.config['BGPScheduler']['SchedulerTopic']}", 
                        msgpack.packb({
                            'prefix': p,
                            'value': self.prefixes[p] 
                        }, use_bin_type=True), 
                        callback = self._delivery_report,
                        timestamp = ts,
                        key=p.encode('utf-8')
                    )
                    self.producer.poll(0)
                    break
                except BufferError as e:
                    logging.error(e)
                    self.producer.poll(1)
        
        logging.info(f"[BGPScheduler] producer message at {ts2dt(ts//1000)} total of {len(self.prefixes)}" )   
        self.producer.flush()

    def _send_end(self, error) :
        batchNum = int(self.config['BGPScheduler']['PartitionNumber'])
        for i in range(batchNum) : 
            self.producer.produce(
                f"{self.topic_header}_{self.config['BGPScheduler']['SchedulerTopic']}", 
                partition=i,
                value = msgpack.packb({
                    'end': True ,
                    'error' : error
                }, use_bin_type=True), 
                callback = self._delivery_report
            )
            self.producer.poll(0) 
        self.producer.flush()


if __name__ == '__main__':
    text = "This script will act as a scheduler and summarize all the data \
recieve from the BGPAnalyzer. It would then producer the aggregated data next \
to the zombieDetector"

    parser = argparse.ArgumentParser(description=text)
    parser.add_argument("--collectors", "-c", help="Choose a collector to push data for (all for rrc00-rrc23)")
    parser.add_argument("--startTime", "-s", help="Choose start time (Format: Y-m-dTH:M:S; Example: 2017-11-06T16:00:00)")
    parser.add_argument("--endTime", "-e", help="Choose end time (Format: Y-m-dTH:M:S; Example: 2017-11-06T16:00:00)")
    args = parser.parse_args()

    start = arrow.get(args.startTime)
    end = arrow.get(args.endTime)
    topic_header = "{}_{:02d}".format(start.year, start.month)

    assert start.hour % 8 == 0, "You must download rib file at 8:00am, 16:00pm or 24:00am"

    collectors = []
    if args.collectors == 'all' :
        for i in range(24) :
            if i == 2 or i == 17 or i == 8 or i == 9 :
                continue
            elif i == 22 or i == 23 : 
                continue
            # elif (i == 22 or i == 23) and start.naive < datetime.datetime(2018,1,1) : 
            #     continue
            collectors.append("rrc{:02d}".format(i))
    else : 
        for rrc in args.collectors.split(",") : 
            collectors.append(rrc.strip())

    BGPScheduler(collectors, start.naive, end.naive, topic_header).run()
