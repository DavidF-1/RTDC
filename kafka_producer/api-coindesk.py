#!/usr/bin/env python3

import json
import time
from datetime import datetime, timezone
import urllib.request
from kafka import KafkaProducer

def main():
	url = "http://api.coindesk.com/v1/bpi/currentprice.json"
	producer = KafkaProducer(bootstrap_servers=['localhost:9092', 'localhost:9093'])

	while True:
		bpiDict = {}
		response = urllib.request.urlopen(url)
		bpicoindesk = json.loads(response.read().decode())
		updatedTime = bpicoindesk["time"]["updatedISO"]
		eurRateFloat = bpicoindesk["bpi"]["EUR"]["rate_float"]
		epochR = int(datetime(int(updatedTime[0:4]), int(updatedTime[5:7]), int(updatedTime[8:10]), 
			int(updatedTime[11:13]),int(updatedTime[14:16]), int(updatedTime[17:19]), tzinfo=timezone.utc).timestamp())
		bpiDict["bpi_time"] = epochR
		bpiDict["bpi_price"] = eurRateFloat
		producer.send("bpi", json.dumps(bpiDict).encode())
		time.sleep(1)

if __name__ == "__main__":
	main()
