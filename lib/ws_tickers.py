import json, time
from websocket import create_connection
import logging

logging = logging.getLogger(__name__)
RUN_TYPE = "PRODUCTION"

class Kraken():
	def __init__(self, pair):
		self.pair = pair

	def create_connect(self, run_type):
		if run_type == "SANDBOX":
			CONNECTION = "wss://ws-sandbox.kraken.com"
		elif run_type == "PRODUCTION":
			CONNECTION = "wss://ws.kraken.com"
		
		for i in range(3):
			try:
				ws = create_connection(CONNECTION)
			except Exception as error:
				print('Caught this error: ' + repr(error))
				time.sleep(3)
			else:
				break
		return ws

	def api_json(self, subscription_type, pair, depth=None, interval=None):
		if subscription_type == "ticker":
			_json_file = {
				"event": "subscribe",
				"pair": [pair],
				"subscription": {"name": "ticker"}
			}
		elif subscription_type == "spread":
			_json_file = {
				"event": "subscribe",
				"pair": [pair],
				"subscription": {"name": "spread"}
			}
		elif subscription_type == "trade":
			_json_file = {
				"event": "subscribe",
				"pair": [pair],
				"subscription": {"name": "trade"}
			}
		elif subscription_type == "book":
			try:
				_json_file = {
					"event": "subscribe",
					"pair": [pair],
					"subscription": {"name": "book", "depth": depth}
				}
			except Exception as error:
				print(error)
		elif subscription_type == "ohlc":
			try:
				_json_file = {
					"event": "subscribe",
					"pair": [pair],
					"subscription": {"name": "ohlc", "interval": interval}
				}
			except Exception as error:
				print(error)
		return _json_file

	def ticker(self, ws, pair):
		return ws.send(json.dumps(self.api_json("ticker", pair=pair)))

	def trade(self, ws, pair):
		return ws.send(json.dumps(self.api_json("trade", pair=pair)))

	def book(self, ws, pair, depth):
		return ws.send(json.dumps(self.api_json("book", depth=depth)))

	def ohlc(self, ws, pair, interval):
		return ws.send(json.dumps(self.api_json("ohlc", interval=int(interval))))

if __name__ == "__main__":
	k = Kraken("XBT/USD")
	ws = k.create_connect(RUN_TYPE)
	k.ticker(ws, "XBT/USD")
	while True:
		try:
			result = ws.recv()
			result = json.loads(result)
			print ("Received '%s'" % result, time.ctime())
		except Exception as error:
			print('Caught this error: ' + repr(error))
			time.sleep(3)

