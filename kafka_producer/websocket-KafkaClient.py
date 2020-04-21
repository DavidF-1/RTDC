#!/usr/bin/env python3
# Connect to a websocket powered by blockchain.info and print events in
# the terminal in real time.
import json
from time import time
import requests
from bs4 import BeautifulSoup
import websocket # install this with the following command: pip install websocket-client
import kafka
from kafka import KafkaProducer

def main():
    ws = open_websocket_to_blockchain()
    producer = KafkaProducer(bootstrap_servers=['localhost:9092', 'localhost:9093'])
    last_ping_time = time()
    while True:
        utxDict = {}
        # Receive event
        data = json.loads(ws.recv())
        # We ping the server every 10s to show we are alive
        if time() - last_ping_time >= 10:
            ws.send(json.dumps({"op": "ping"}))
            last_ping_time = time()
        # Response to "ping" events
        if data["op"] == "pong":
            pass
        # New unconfirmed transactions
        elif data["op"] == "utx":
            transaction_timestamp = data["x"]["time"]
            utxDict["transaction_timestamp"] = transaction_timestamp

            transaction_hash = data['x']['hash'] # this uniquely identifies the transaction
            utxDict["transaction_hash"] = transaction_hash

            transaction_total_amount = 0
            for recipient in data["x"]["out"]:
                # Every transaction may in fact have multiple recipients
                # Note that the total amount is in hundredth of microbitcoin; you need to
                # divide by 10**8 to obtain the value in bitcoins.
                transaction_total_amount += recipient["value"] / 100000000.
            utxDict["transaction_total_amount"]=transaction_total_amount
            producer.send("utx", json.dumps(utxDict).encode())
        # New block
        elif data["op"] == "block":
            i = 0
            j = 0
            minerName = ""
            block_hash = data['x']['hash']
            utxDict["block_hash"] = block_hash
            block_timestamp = data["x"]["time"]
            utxDict["block_timestamp"] = block_timestamp
            url = 'https://www.blockchain.com/btc/block/' + block_hash
            r = requests.get(url)
            responseText = r.text
            soup = BeautifulSoup(responseText, 'html.parser')
            soup.prettify()
            for div in soup.find_all('div'):
                i += 1
                content = div.get_text()
                if (content == "Miner" or content == "miner"):
                    print("********************************")
                    print(i)
                    break
            for divSecond in soup.find_all('div'):
                j += 1
                if j == (i + 2):
                    if (divSecond.get_text() != "Miner" or divSecond.get_text() != 'miner'):
                        minerName = divSecond.get_text()
                        print("||||||||||||||||||||||||||||")
                        print(minerName)
                    else:
                        continue
            block_found_by = minerName
            utxDict["block_found_by"] = block_found_by
            block_reward = 12.5 # blocks mined in 2016 have an associated reward of 12.5 BTC
            utxDict["block_reward"] = block_reward
            producer.send("blk", json.dumps(utxDict).encode())
            print("{} New block {} found by {}".format(block_timestamp, block_hash, block_found_by))
        	# This really should never happen
        else:
            print("Unknown op: {}".format(data["op"]))

def open_websocket_to_blockchain():
    # Open a websocket
    ws = websocket.WebSocket()
    ws.connect("wss://ws.blockchain.info/inv")
    # Register to unconfirmed transaction events
    ws.send(json.dumps({"op":"unconfirmed_sub"}))
    # Register to block creation events
    ws.send(json.dumps({"op":"blocks_sub"}))
    return ws

if __name__ == "__main__":
    main()
