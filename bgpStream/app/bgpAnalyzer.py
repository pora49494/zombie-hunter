import arrow
import msgpack
import datetime
import argparse
import configparser
import logging
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError, TopicPartition, Producer

#debug
import random

def dt2ts(dt):
    return int((dt - datetime.datetime(1970, 1, 1)).total_seconds())

def ts2dt(ts):
    return datetime.datetime.fromtimestamp(ts + datetime.datetime(1970, 1, 1).timestamp())

class BGPAnalyzer:
    ''' BGPAnalyzer consume data from bgpProducer. It analyze and summarize
    number of peers which annouce prefix for each interval, the reciever of
    this producer it to the bgpScheduler. '''

    def __init__(self, collector, start, end, topic_header):
        self.start = start
        self.end = end
        self.collector = collector
        self.topic_header = topic_header
        
        self.prefixes = defaultdict(set)

        self.config = configparser.ConfigParser()
        self.config.read('/app/config.ini')

        FORMAT = '%(asctime)s BGPAnalyzer %(message)s'
        logging.basicConfig(
            format=FORMAT, filename=f'{self.config["DEFAULT"]["LogLocation"]}/{start.year}-{start.month}-ihr-kafka-BGPProducer-{self.collector}.log',
            level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S'
        )

        self.producer = Producer({
            'bootstrap.servers': self.config['DEFAULT']['KafkaServer'],
            'default.topic.config': {
                'compression.codec': 'snappy',
                'queue.buffering.max.messages': 1000000,
            },
            'batch.num.messages': 10000*10,
            'message.max.bytes': 1000000*10,
            'linger.ms': 20.0
        })

    def get_consumer(self, topic):
        ''' get_consumer() create a consumer interface and set the offset to the 
        start timestamp. '''

        consumer = Consumer({
            'bootstrap.servers': self.config['DEFAULT']['KafkaServer'],
            'group.id': 'mygroup',
            'client.id': 'client-1',
            'enable.auto.commit': True,
            'session.timeout.ms': 6000,
            'max.poll.interval.ms': 60000000,
            'default.topic.config': {'auto.offset.reset': 'smallest'}
        })
        topicPartitions = [TopicPartition(topic, 0, dt2ts(self.start)*1000 )]
        offsetsTimestamp = consumer.offsets_for_times(topicPartitions)
        consumer.assign(offsetsTimestamp)
        return consumer

    def _consume_rib(self):
        ''' _consume_rib() consume rib data from the given topic. '''
        
        topic = f"{self.topic_header}_ihr_bgp_{self.collector}_ribs"
        
        logging.info(f"[BGPAnalyzer-{self.collector}] start consuming rib data")
        consumer = self.get_consumer(topic)

        try:
            end_ts = dt2ts(self.end) * 1000
            while True:
                msg = consumer.poll(0.1)
                if msg is None: 
                    continue

                elif not msg.error():
                    if msg.timestamp()[1] > end_ts :
                        logging.info(f'[{topic}] exceeding time window')
                        break

                    message = msgpack.unpackb(msg.value(), raw=False)    
                    if 'end' in message : 
                        logging.info(f'[{topic}] recieving END message')                    
                        break

                    self._process(message['record'])

                elif msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.info(f'[{topic}] end of partition reached {msg.partition()}')
                else:
                    logging.error(f'[{topic}] : {msg.error().str()}')
        
        except KeyboardInterrupt:
            logging.info("recieve ctrl-c exit")
            return
        except Exception as e :
            logging.error(f"[BGPAnalyzer-{self.collector}] exit with error : {e}")
            return 
        
        finally:
            consumer.close()
            logging.info(f"[BGPAnalyzer-{self.collector}] done consuming rib data")

    def _consume_upd(self):
        logging.info(f"[BGPAnalyzer-{self.collector}] start comsuming updates")
        
        topic = f"{self.topic_header}_ihr_bgp_{self.collector}_updates"
        consumer = self.get_consumer(topic)

        interval = int(self.config['DEFAULT']['Interval'])
        offset = dt2ts(self.start)*1000 + interval
        largeTimeWindow = int(self.config['BGPAnalyzer']['LargeTimeWindow'])

        try:
            end_ts = dt2ts(self.end) * 1000
            while True:
                msg = consumer.poll(0.1)

                if msg is None:
                    continue
                elif not msg.error():
                    
                    if msg.timestamp()[1] > end_ts :
                        logging.info(f'[{topic}] exceeding time window')
                        break

                    # pause = False
                    if msg.timestamp()[1] - offset > largeTimeWindow:
                        logging.warning(f"[{topic}] recieve an out of order message at {ts2dt(msg.timestamp()[1]//1000)} : Drop")
                        continue
                        # pause = True
                        
                        # consumer.pause([TopicPartition(msg.topic(), 0)])
                        # logging.info(f"[{topic}] LARGE_WINDOW pause consuming")

                    while offset < msg.timestamp()[1]:
                        self._produce(offset)
                        offset += interval

                    # if pause:
                    #     consumer.resume([TopicPartition(msg.topic(), 0)])
                    #     logging.info(f"[{topic}] LARGE_WINDOW end, resume consuming")

                    message = msgpack.unpackb(msg.value(), raw=False)
                    if 'end' in message :
                        logging.info(f'[{topic}] recieve end message')
                        break

                    self._process(message['record'])

                elif msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.info(f'[{topic}] end of partition reached {msg.partition()}')
                else:
                    logging.error(f'[{topic}] : {msg.error().str()}')

        except KeyboardInterrupt:
            logging.info("recieve ctrl-c exit")
            return

        finally:
            while offset <= dt2ts(self.end) * 1000 :
                self._produce(offset)
                offset += interval
                
            consumer.close()
            logging.info(f"[BGPAnalyzer-{self.collector}] done comsuming updates")

    def run(self):
        ''' BGPAnalyzer consuming rib and update, summarize it then publish. '''
        self._consume_rib()
        self._consume_upd()

    def _process(self, record):
        ''' _process check the number of peer which announce prefix. In self.prefix[p] contain
        the set with peer addess as the member. If the record is 'A' or 'R' add it to the set. 
        If it is 'W" remove it.
        '''
        rec = record["rec"]
        elements = record["elements"]
        if rec["type"] == "unknown":
            return

        for elem in elements:
            if elem["type"] == "R" or elem["type"] == "A":
                p = elem["fields"]["prefix"]
                self.prefixes[p].add(elem["peer_address"])

            elif elem["type"] == "W":
                p = elem["fields"]["prefix"]
                if elem["peer_address"] in self.prefixes[p]:
                    self.prefixes[p].remove(elem["peer_address"])
                else:
                    logging.debug(
                        f'[BGPAnalyzer-{self.collector}] try to remove non-exist peer {elem["peer_address"]}')

    def _delivery_report(self, err, msg):
        ''' Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). '''
        if err is not None:
            logging.debug(f'[BGPAnalyzer-{self.collector}] message delivery failed: {err}')
        else:
            pass

    def _produce(self, offset):
        topicName = f"{self.topic_header}_ihr_bgp_summary_{self.collector}"

        for p in self.prefixes:
            self.producer.produce(
                topicName,
                msgpack.packb({
                    'prefix': p,
                    'value': len(self.prefixes[p])
                }, use_bin_type=True),
                callback=self._delivery_report,
                timestamp=offset
            )
            self.producer.poll(0)

        self.producer.produce(
            topicName,
            msgpack.packb({
                'end': True,
                'topic': topicName
            }, use_bin_type=True),
            callback=self._delivery_report,
            timestamp=offset
        )

        logging.debug(f"[{topicName}] time: {ts2dt(offset//1000)}")
        self.producer.flush()


if __name__ == '__main__':
    text = "This script will analyze all bgp data sent by BGPProducer."

    parser = argparse.ArgumentParser(description=text)
    parser.add_argument("--collector", "-c", help="Choose a collector to push data for")
    parser.add_argument("--startTime", "-s", help="Choose start time (Format: Y-m-dTH:M:S; Example: 2017-11-06T16:00:00)")
    parser.add_argument("--endTime", "-e", help="Choose end time (Format: Y-m-dTH:M:S; Example: 2017-11-06T16:00:00)")

    args = parser.parse_args()

    start = arrow.get(args.startTime)
    end = arrow.get(args.endTime)
    topic_header = "{}_{:02d}".format(start.year, start.month)

    assert start.hour % 8 == 0, "You must download rib file at 8:00am, 16:00pm or 24:00am"

    BGPAnalyzer(args.collector, start.naive, end.naive, topic_header).run()
