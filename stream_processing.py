# - read data from kafka
# - process
# - send data back to kafka

# -kafka location,kafka topic 
import atexit
import argparse
import logging
import json
import time

from kafka import KafkaProducer
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

logging.basicConfig()
logger = logging.getLogger('stream-processing')
logger.setLevel(logging.INFO)

kafka_producer = None
topic = None
kafka_broker = None
new_topic = None 

def shutdown_hook(producer):
	logger.info('prepare to shutdown producer')
	kafka_producer.flush(10)
	producer.close(10)

def process(timeobject, rdd):
	# - count average
	num_of_records = rdd.count()
	if num_of_records == 0:
		return
	price_sum =  rdd.map(lambda record : float(json.load(record[1].decode('utf-8'))[0].get('LastTradePrice'))).reduce(lambda a, b: a + b)
	average = price_sum / num_of_records
	logger.info('Received %d records from kafka, average price is %f' % (num_of_records, average))
	current_time = time.time()
	data = json.dumps({
		'timestamp' : current_time,
		'average' : average

		})

	try: 
		kafka_producer.send(new_topic, value = data)
	except Exception:
		logger.warn('Failed to send data')








if _name_ == '_main_':
	# - setup command line argument
	parser = argparse.ArgumentParser()
	parser.argument('kafka_broker', help = 'location of kafka')
	parser.argument('topic', help = 'original topic name')
	parser.argument('new topic', help = 'new topic to send data to')

	# - get arguments
	args = parser.parse_args()
	kafka_broker = args.kafka_broker
	topic = args.topic
	new_topic = args.new_topic

	kafka_producer = KafkaProducer(bootstrap_servers = kafka_broker)


	# setup spark streaming utilize

	# - create SparkContext and StreamingContext
    sc = SparkContext("local[2]", "StockAveragePrice")
    sc.setLogLevel('ERROR')
    ssc = StreamingContext(sc, 5)

    # - instantiate a kafka stream for processing
    directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': kafka_broker})
    kafkaStream.foreachRDD(process)
    process_stream(directKafkaStream)

    atexit.register(shutdown_hook, kafka_producer)
    ssc.start()
    ssc.awaitTermination()

