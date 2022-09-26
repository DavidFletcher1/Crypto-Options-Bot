import pandas as pd
import requests
import asyncio
import websockets
import json
import time
from discord import Webhook, RequestsWebhookAdapter



lookup_fair_price = {}
lookup_contract_id_ftx = {}
previous_lookup = time.time()
units_processed = 0
options_processed_cache_15 = {}
options_processed_cache_day = {}
daily_hour = 13
daily_minute = 15

DISCORD_15_MINS_WEBHOOK = "https://discord.com/api/webhooks/946614862371504151/CgRfnxP8dfgYHXYnk0tj3gV8sGadtFj-llYoH3iWhCOvNhqXD8rTpTCiQHRXRVUGe9t3"
DISCORD_DAY_WEBHOOK = "https://discord.com/api/webhooks/946632070279594024/Wutv0POmvfS8OITCqyic3r1wBsIL7KijUpt5nfUoIwWpGeysXrY5UqgQfpqVhPkWuRz_"
DISCORD_15_2 = "https://discord.com/api/webhooks/988882525428514846/Z7yRWZu0161JNLne7uPYxb9uOelbs_0RsSLXJv9xaPYSUkD5kguRcxzibHe1Pc3bYz6-"
DISCORD_DAY_2 = "https://discord.com/api/webhooks/988883140506447982/M3_5B5HQe6BuoDSU7AK7tzfYBmEemQctclwjLoC3ueGYaWjSYxVIuwEpxOi6WSXlU6IW"

def fill_btc_fair():
	global previous_lookup
	CURR_BTC_PRICE = requests.get("https://www.deribit.com/api/v2/public/get_index_price?index_name=btc_usd").json()['result']['index_price']
	print("Initializing fair market data... \nWhile you wait here is the current price of BTC: " + str(CURR_BTC_PRICE))
	get_options_btc = requests.get("https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency=BTC&kind=option").json()['result']
	for option in get_options_btc:
		instr_name = option['instrument_name']
		if option['bid_price'] != None and option['ask_price'] != None:
			# some significant figures for bid/ask are completely inaccurate
			fair_market_bid = option['bid_price'] * CURR_BTC_PRICE
			fair_market_ask = option['ask_price'] * CURR_BTC_PRICE
			fair_market_midprice = option['mid_price'] * CURR_BTC_PRICE
			lookup_fair_price[instr_name] = (fair_market_bid, fair_market_ask, fair_market_midprice)
	previous_lookup = time.time()

def fill_eth_fair():
	global previous_lookup
	CURR_ETH_PRICE = requests.get("https://www.deribit.com/api/v2/public/get_index_price?index_name=eth_usd").json()['result']['index_price']
	print("And here is the current price of ETH: " + str(CURR_ETH_PRICE))
	get_options_eth = requests.get("https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency=ETH&kind=option").json()['result']
	for option in get_options_eth:
		instr_name = option['instrument_name']
		if option['bid_price'] != None and option['ask_price'] != None:
			fair_market_bid = option['bid_price'] * CURR_ETH_PRICE
			fair_market_ask = option['ask_price'] * CURR_ETH_PRICE
			fair_market_midprice = option['mid_price'] * CURR_ETH_PRICE
			lookup_fair_price[instr_name] = (fair_market_bid, fair_market_ask, fair_market_midprice)
	previous_lookup = time.time()


async def establish_ftx_connection():
    async with websockets.connect("wss://api.ledgerx.com/ws") as websocket:
        async for message in websocket:
        	process_datapoint(json.loads(message))


def process_datapoint(datapoint):
	global units_processed
	global previous_lookup
	global options_processed_cache_15
	global options_processed_cache_day
	year, month, day, hour, minute = map(int, time.strftime("%Y %m %d %H %M").split())

	curr_time = time.time()
	units_processed+=1

	if curr_time - previous_lookup > 5:
		print("Units processed ..... " + str(units_processed))
		print("Updating.......")
		fill_btc_fair()
		fill_eth_fair()

	socket_result_type = datapoint['type']
	# check if it is a market update
	if socket_result_type == 'book_top':
		contractid = datapoint['contract_id']
		if contractid in lookup_contract_id_ftx:
			trading_tag = lookup_contract_id_ftx[contractid]
			if trading_tag in lookup_fair_price:
				fair_trading_price = lookup_fair_price[trading_tag]
				ftx_trading_price = (datapoint['bid'] / 100, datapoint['ask'] / 100)
				fair_midpoint = fair_trading_price[2]
				# [0] is bid, [1] is ask, [2] is midpoint
				percentage_bid_diff = ftx_trading_price[0] / fair_midpoint
				percentage_ask_diff = ftx_trading_price[1] / fair_midpoint
				deribit_bid_ask_ratio = fair_trading_price[0]/fair_trading_price[1]

				if percentage_bid_diff >= 1.1 and deribit_bid_ask_ratio > .8:
					if not (trading_tag in options_processed_cache_day and time.time() - options_processed_cache_day[trading_tag] < 86400):
						print("FOUND BID TARGET")
						print("Fair price for bid of " + trading_tag + ": " + str(fair_trading_price[0]) + " vs FTX bid: " + str(ftx_trading_price[0]))
						print("Fair price for ask of " + trading_tag + ": " + str(fair_trading_price[1]) + " vs FTX ask: " + str(ftx_trading_price[1]))
						print("Fair price for midpoint of " + trading_tag + ": " + str(fair_midpoint))
						print("Bid percentage difference to midpoint: " + str(percentage_bid_diff))
					if hour == daily_hour and (minute == daily_minute or minute == daily_minute):
						if not (trading_tag in options_processed_cache_day and time.time() - options_processed_cache_day[trading_tag] < 86400):
							message_discord(f'{trading_tag} GOOD BID \n\tBID = {str(round(ftx_trading_price[0], 1))}. DBM = {str(round(fair_midpoint, 1))}.  % OFF = {str(round(((ftx_trading_price[0]/fair_midpoint)-1)*100, 1))}%.', DISCORD_DAY_WEBHOOK)
							message_discord(f'{trading_tag} GOOD BID \n\tBID = {str(round(ftx_trading_price[0], 1))}. DBM = {str(round(fair_midpoint, 1))}.  % OFF = {str(round(((ftx_trading_price[0]/fair_midpoint)-1)*100, 1))}%.', DISCORD_DAY_2)
							options_processed_cache_day[trading_tag] = time.time()
					else:
						if not (trading_tag in options_processed_cache_day and time.time() - options_processed_cache_day[trading_tag] < 86400):
							if not (trading_tag in options_processed_cache_15 and time.time() - options_processed_cache_15[trading_tag] < 900):
								message_discord(f'{trading_tag} GOOD BID \n\tBID = {str(round(ftx_trading_price[0], 1))}. DBM = {str(round(fair_midpoint, 1))}.  % OFF = {str(round(((ftx_trading_price[0]/fair_midpoint)-1)*100, 1))}%.', DISCORD_15_MINS_WEBHOOK)
								message_discord(f'{trading_tag} GOOD BID \n\tBID = {str(round(ftx_trading_price[0], 1))}. DBM = {str(round(fair_midpoint, 1))}.  % OFF = {str(round(((ftx_trading_price[0]/fair_midpoint)-1)*100, 1))}%.', DISCORD_15_2)
								options_processed_cache_15[trading_tag] = time.time()
					# do stuff
				if percentage_ask_diff <= 0.9 and deribit_bid_ask_ratio > .8 and ftx_trading_price[1] != 0:
					if not (trading_tag in options_processed_cache_day and time.time() - options_processed_cache_day[trading_tag] < 86400):
						print("FOUND ASK TARGET")
						print("Fair price for bid of " + trading_tag + ": " + str(fair_trading_price[0]) + " vs FTX bid: " + str(ftx_trading_price[0]))
						print("Fair price for ask of " + trading_tag + ": " + str(fair_trading_price[1]) + " vs FTX ask: " + str(ftx_trading_price[1]))
						print("Fair price for midpoint of " + trading_tag + ": " + str(fair_midpoint))
						print("Ask percentage difference to midpoint: " + str(percentage_ask_diff))

					# 
					if hour == daily_hour and (minute == daily_minute or minute == daily_minute + 1):
						if not (trading_tag in options_processed_cache_day and time.time() - options_processed_cache_day[trading_tag] < 86400):
							message_discord(f'{trading_tag} GOOD BID \n\tBID = {str(round(ftx_trading_price[0], 1))}. DBM = {str(round(fair_midpoint, 1))}.  % OFF = {str(round(((ftx_trading_price[0]/fair_midpoint)-1)*100, 1))}%.', DISCORD_DAY_WEBHOOK)
							message_discord(f'{trading_tag} GOOD BID \n\tBID = {str(round(ftx_trading_price[0], 1))}. DBM = {str(round(fair_midpoint, 1))}.  % OFF = {str(round(((ftx_trading_price[0]/fair_midpoint)-1)*100, 1))}%.', DISCORD_DAY_2)
							options_processed_cache_day[trading_tag] = time.time()
					else:
						if not (trading_tag in options_processed_cache_day and time.time() - options_processed_cache_day[trading_tag] < 86400):
							if not (trading_tag in options_processed_cache_15 and time.time() - options_processed_cache_15[trading_tag] < 900):
								message_discord(f'{trading_tag} GOOD BID \n\tBID = {str(round(ftx_trading_price[0], 1))}. DBM = {str(round(fair_midpoint, 1))}.  % OFF = {str(round(((ftx_trading_price[0]/fair_midpoint)-1)*100, 1))}%.', DISCORD_15_MINS_WEBHOOK)
								message_discord(f'{trading_tag} GOOD BID \n\tBID = {str(round(ftx_trading_price[0], 1))}. DBM = {str(round(fair_midpoint, 1))}.  % OFF = {str(round(((ftx_trading_price[0]/fair_midpoint)-1)*100, 1))}%.', DISCORD_15_2)
								options_processed_cache_15[trading_tag] = time.time()



def message_discord(message, url):
	webhook = Webhook.from_url(url, adapter=RequestsWebhookAdapter())
	webhook.send(message)
	#print(message)

def parse_ftx_option_label(slabel):
	newl = slabel.replace('Mini-','').replace('Call','C').replace('Put','P').replace('2022', '22')
	return newl

def fill_ftx_contract_lookup():
	print("Initializing contract id lookup...")
	contract_id_ftx_load = requests.get('https://api.ledgerx.com/trading/contracts?derivative_type=options_contract&limit=100000').json()['data']
	#contract_id_ftx_load = requests.get('https://api.ledgerx.com/trading/contracts?limit=100000').json()['data']
	for contract in contract_id_ftx_load:
		parsed_label = parse_ftx_option_label(contract['label'])
		lookup_contract_id_ftx[contract['id']] = parsed_label

def main():
	fill_btc_fair()
	fill_eth_fair()
	fill_ftx_contract_lookup()
	asyncio.run(establish_ftx_connection())

if __name__ == '__main__':
	main()
