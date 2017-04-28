# -1. read from specific kafka cluster
# -2. write to specific cassandra cluster and topic
# -3. data need to be orgainzed according to SSTable

from cassandra.cluster import cluster
from kafka import KafkaConsumer
import atexit
import argparse
import logging

logging.basicConfig()
logger = logging.getLogger('data-storage')
logger.setLevel(logging.DEBUG)

def persist_data(stock_data, cassandra_session,table):
	logger.debug('Start to persist data to cassandra %s', stock_data)
    parsed = json.loads(stock_data)[0]
    symbol = parsed.get('StockSymbol')
    price = float(parsed.get('LastTradePrice'))
    tradetime = parsed.get('LastTradeDateTime')

    # - perpare to insert to cassandra
    statement = "INSERT INTO %s (stock_symbol, trade_time, trade_price) VALUES ('%s', '%s', %f)"  % (table, symbol, trade_time, price)
    cassandra_session.execute(statement)
    logger.info('Persisted data to cassandra for symbol %s, price %f, trade_time %s' % (symbol, price, trade_time))

def shutdown_hook(consumer, session):
    """
    a shutdown hook to be called before the shutdown
    :param consumer: instance of a kafka consumer
    :param session: instance of a cassandra session
    :return: None
    """
    try:
        logger.info('Closing Kafka Consumer')
        consumer.close()
        logger.info('Kafka Consumer closed')
        logger.info('Closing Cassandra Session')
        session.shutdown()
        logger.info('Cassandra Session closed')
    except KafkaError as kafka_error:
        logger.warn('Failed to close Kafka Consumer, caused by: %s', kafka_error.message)
    finally:
        logger.info('Existing program') 


if _name_ == '_main_':
	# - setup command line arguments
	parser = argparse.ArgumentParser()
	
	# - default kafka topic to read from
	parser.add_argument('topic_name', help='the Kafka topic to subscribe form')
	
	# - default kafka broker location
	parser.add_argument('kafka_broker', help='the location of kafka broker')

	# - default keyspace to use
	parser.add_argument('key_space', help='the keyspace to write data to')

	# - default table to use
	parser.add_argument('data_table', help='the data table to use')

	parser.add_argument('cassandra_broker', help='the cassandra_broker location')


	# - parse arguments
	args = parser.parse_args()
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker
	key_space = args.key_space
	data_table = args.data_table
	# - assume 127.0.0.1, 127.0.0.1
	cassandra_broker = args.cassandra_broker

	# - initiate a simple kafka consumer
	consumer = KafkaConsumer(
		topic_name,
		bootstrap_servers=kafka_broker
	)

	# - initiate a cassandra session
	cassandra_cluster = Cluster(
		contact_points=cassandra_broker.split(',')

	)
	session = cassandra_cluster.connect()

	# - check if keyspace datatable is created
	# - create keyspace if not exist
	session.execute(
		"CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND durable_write = 'true' " % key_space
	)
	session.set_keyspace(key_space)
	session.execute(
		"CREATE TABLE IF NOT EXISTS %s (stock_symbol text, trade_time timestamp, trade_price float, PRIMARY KEY(stock_symbol, trade_time)) "% data_table
	)

	# - setup proper shutdown hook
    atexit.register(shutdown_hook, consumer, session)
    
	for msg in sonsumer: 
		persist_data(msg.value, session, data_table)







