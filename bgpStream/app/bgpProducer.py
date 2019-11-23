import sys
import argparse 
import arrow
import msgpack
import configparser
import logging
from datetime import datetime, timedelta
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from _pybgpstream import BGPStream, BGPRecord

MAX_ELEMENT = 10

def dt2ts(dt):
    return int((dt - datetime(1970, 1, 1)).total_seconds())

def ts2dt(ts):
    return datetime.fromtimestamp(ts + datetime(1970, 1, 1).timestamp())

def getElementDict(element):
    elementDict = dict()
    elementDict["type"] = element.type
    elementDict["peer_asn"] = element.peer_asn
    elementDict["peer_address"] = element.peer_address
    elementDict["fields"] = element.fields
    if 'communities' in element.fields:
        elementDict['fields']['communities'] = list(
            element.fields['communities'])

    return elementDict

def getRecordDict(record):
    recordDict = dict()
    recordDict["project"] = record.project
    recordDict["collector"] = record.collector
    recordDict["type"] = record.type
    recordDict["dump_time"] = record.dump_time
    recordDict["time"] = record.time
    recordDict["status"] = record.status
    recordDict["dump_position"] = record.dump_position

    return recordDict

class BGPProducer:
    def __init__(self, record_type, collector, start, end, topic_header):
        self.record_type = record_type
        self.collector = collector
        self.start = start
        self.end = end
        self.topic_header = topic_header

        self.config = configparser.ConfigParser()
        self.config.read('/app/config.ini')

        FORMAT = '%(asctime)s BGPProducer %(message)s'
        logging.basicConfig(
            format=FORMAT, filename=f'{self.config["DEFAULT"]["LogLocation"]}/{start.year}-{start.month}-ihr-kafka-BGPProducer-{self.collector}.log',
            level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S'
        )

    def _delivery_report(self, err, msg):
        ''' Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). '''
        if err is not None:
            logging.debug(f'message delivery failed: {err}')
        else:
            pass

    def getBGPStream(self):
        logging.info(
            f"creating BGPstream {self.collector} {self.record_type} {self.start.year}-{self.start.month} ")

        stream = BGPStream()
        stream.add_filter('collector', self.collector)
        stream.add_filter('record-type', self.record_type)
        if self.record_type == "updates":
            stream.add_interval_filter(dt2ts(self.start), dt2ts(self.end))
        elif self.record_type == "ribs":
            _start = dt2ts(self.start - timedelta(hours=2))
            _end = dt2ts(self.start + timedelta(hours=2))
            stream.add_interval_filter(_start, _end)

        return stream

    def _create_topic(self):
        topicName = f"{self.topic_header}_ihr_bgp_{self.collector}_{self.record_type}"
        logging.info(f"[{topicName}] try to create topic")

        admin_client = AdminClient({
            'bootstrap.servers': self.config['DEFAULT']['KafkaServer']
        })
        topic_list = [NewTopic(topicName, num_partitions=1, replication_factor=1)]
        created_topic = admin_client.create_topics(topic_list)

        for topic, f in created_topic.items():
            try:
                f.result()  # The result itself is None
                logging.info(f"[{topic}] topic created")
            except Exception as e:
                logging.error(f"[{topic}] failed to create topic: {e}")

        return topicName

    def produce(self):
        # debug
        debuger = dt2ts(self.start) * 1000  

        stream = self.getBGPStream()
        topic = self._create_topic()
        producer = Producer({
            'bootstrap.servers': self.config['DEFAULT']['KafkaServer'],
            'default.topic.config': {
                'compression.codec': 'snappy',
                'queue.buffering.max.messages': 1000000,
            },
            'batch.num.messages': 10000*10,
            'message.max.bytes': 1000000*10
        })

        rec = BGPRecord()
        stream.start()

        logging.info(f"[{topic}] start producing")
        try:
            while stream and stream.get_next_record(rec):
                
                if rec.time > dt2ts(self.end):
                    logging.error(f"[{topic}] time window exceed")
                    break
                    
                if rec.status != "valid":
                    continue

                completeRecord = dict()
                completeRecord["rec"] = getRecordDict(rec)
                recordTimeStamp = int(rec.time) * 1000
                
                completeRecord["elements"] = []
                elem = rec.get_next_elem()
                count = 0 

                while(elem):
                    elementDict = getElementDict(elem)
                    completeRecord["elements"].append(elementDict)
                    count += 1

                    if count > MAX_ELEMENT :
                        producer.produce(
                            topic,
                            msgpack.packb({
                                'record': completeRecord
                            }, use_bin_type=True),
                            callback=self._delivery_report,
                            timestamp=recordTimeStamp
                        )
                        producer.poll(0)
                        completeRecord["elements"] = []
                        count = 0 

                    elem = rec.get_next_elem()

                if len(completeRecord['elements']): 
                    producer.produce(
                        topic,
                        msgpack.packb({
                            'record': completeRecord
                        }, use_bin_type=True),
                        callback=self._delivery_report,
                        timestamp=recordTimeStamp
                    )
                    producer.poll(0)

                # debug
                if recordTimeStamp > debuger :
                    logging.debug(f"[{topic}] produced at {ts2dt(debuger//1000)}")
                    debuger += 3600000

        except Exception as e :
            logging.error(f"[{topic}] exit with error : {e}")
            return

        finally:        
            producer.produce(
                topic,
                msgpack.packb({'end': True}, use_bin_type=True),
                callback=self._delivery_report,
                timestamp=dt2ts(self.end)*1000
            )   
            producer.poll(0)
            producer.flush()
            logging.info(f"[{topic}] done producing")
            
if __name__ == '__main__':
    text = "This script downloads bgp data during a specific time window and producer it \
to kafka cluster. You must input the type of bgp data you want to download (ribs or update), \
collector number and also the start time and the end time of data you want to produce"

    parser = argparse.ArgumentParser(description=text)
    parser.add_argument("--collector", "-c", help="Choose a collector to push data for")
    parser.add_argument("--startTime", "-s", help="Choose start time (Format: Y-m-dTH:M:S; Example: 2017-11-06T16:00:00)")
    parser.add_argument("--endTime", "-e", help="Choose end time (Format: Y-m-dTH:M:S; Example: 2017-11-06T16:00:00)")
    parser.add_argument("--type", "-t", help="Choose record type: ribs or updates")
    args = parser.parse_args()

    start = arrow.get(args.startTime)
    end = arrow.get(args.endTime)
    topic_header = "{}_{:02d}".format(start.year, start.month) 

    assert start.hour % 8 == 0, "You must download rib file at 8:00am, 16:00pm or 24:00am"
    assert args.type in ["updates","ribs"], "type should be 'updates' or 'ribs'"

    BGPProducer(args.type, args.collector, start.naive, end.naive, topic_header).produce()
