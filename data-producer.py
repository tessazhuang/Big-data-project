# - get data and write to kafka 
import atexit
import logging
import json
from flask import (Flask, jsonify)
from aspscheduler.schedulers.background import BackgroundScheduler
from kafka import KafkaProducer
from googlefinance import getQuotes
from kafka.errors import (KafkaTimeOutError, KafkaError)

logger_format = '%(asctime) - 15s %(message)s'
logging.basticConfit(format = logger_format) 
logger = logging.getLogger(_name_)
# - DEBUG INFO WARNGING ERROR
logger.setLevel(logging.DEBUG)

app = Flask(_name_)
app.config.from_envvar('ENV_CONFIG_FILE')
kafka_broker = app.config['CONFIG_KAFKA_ENDPOINT']
kafka_topic = app.config['CONFIG_KAFKA_TOPIC']

producer = KafkaProducer(bootstrap_servers = kafka_broker)
schedule = BackgroundScheduler()
schedule.add_executor('threadpool')
schedule.start()

symbols = set() 
def shutdown_hook(): 
	# - close kafka producer
	# - scheduler
	logger.info('shutdown kafka producer')
	producer.flush(10)
	producer.close()
	logger.info('shutdown scheduler')
	scheduler.shutdown()




def fetch_price(symbol):
	try: 
		logger.debug('start to fetch stock price for %s', symbol)
		stock_price = json.dumps(getQuotes(symbol))
		logger.debug('Retrieved strock price %s' , stock_price)
		producer.send(topic = kafka_topic, value = stock_price)
		logger.debug('finish write % s price  to kafka', symbol)
	except KafkaTimeoutError as timeout_error:
		logger.error('Failed to send stock price for % s to kafka, caused by : %s', (symbol, timeout_error.message))
	except Expcetion as e:
		logger.error('Failed to send stock price for %s ', symbol)

#fetch_price('AAPL')

@app.route('/', methods=['GET'])
def default():
	return jsonify('ok'),200
# - add stock
@app.route('/<symbol>', methods = ['POST'])
def add_stock(symbol):
	if not symbol:
		return jsonify({
			'error': 'Stock symbol cannot be empty'
			}),400
	if symbol in symbols :
		pass
	else: 
		# - do something
		symbols.add(symbol)
		schedule.add_job(fetch_price, 'interval', ['AAPL'], seconds=1, id='AAPL')
	return jsonify(list(symbols)),200

# - remove stock
@app.route('/<symbol>', methods = ['DELETE'])



def del_stock(symbol):
	if not symbol: 
		return jsonify({
			'error': 'Stock symbol cannot be empty'
			}),400
	if symbol not in symbols:
		pass
	else:
		# - do something
		symbols.remove(symbol)
		schedule.remove_job(symbol)

	return jsonify(list(symbols)),200

# - setup proper shutdown hook
atexit.register(shutdown_hook)

app.run(host= '0.0.0.0'. port = 5000, debug = True)
