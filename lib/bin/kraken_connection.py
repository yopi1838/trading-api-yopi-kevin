import json, time
from websocket import create_connection

for i in range(3):
	try:
		ws = create_connection("wss://ws.kraken.com")
	except Exception as error:
		print('Caught this error: ' + repr(error))
		time.sleep(3)
	else:
		break

ws.send(json.dumps({
	"event": "subscribe",
	#"event": "ping",
	"pair": ["XBT/USD"],
	#"subscription": {"name": "ticker"}
	#"subscription": {"name": "spread"}
	# "subscription": {"name": "trade"}
	#"subscription": {"name": "book", "depth": 10}
	"subscription": {"name": "ohlc", "interval": 1}
}))



while True:
	try:
		result = ws.recv()
		result = json.loads(result)
		print ("Received '%s'" % result, time.ctime())
	except Exception as error:
		print('Caught this error: ' + repr(error))
		time.sleep(3)

ws.close()