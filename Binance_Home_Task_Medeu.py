import requests as r
import json
import os, sys
import time
##--------------------------
from prometheus_client.core import CollectorRegistry
from prometheus_client import start_http_server, REGISTRY
from prometheus_client.metrics_core import CounterMetricFamily, GaugeMetricFamily



API = 'https://binance.com/api/v3/ticker/24hr' #API endpoint with 24hr refreshable data
request = r.request('GET', API)
binance_data = json.loads(request.text)

# Let's create a list with binance values
transaction_list = list()
for transaction in binance_data:
    symbol = transaction['symbol']
    volume = float(transaction['volume'])
    count = transaction['count']
    tbl_struct = {'symbol':symbol, 'volume':volume, 'count':count}
    transaction_list.append(tbl_struct)


#Answer for Q1
#-------------------------------------------------------
def q1_answer():
    q1_sorted = list()
    global q1_symbols
    q1_symbols = list()
    for i in transaction_list:
        if i['symbol'][-3:] == 'BTC':
            q1_sorted.append(i)

    q1_sorted = sorted(q1_sorted, key=lambda k: k['volume'], reverse=True)
    print('Answers for Q1:')
    for i in q1_sorted[:5]:
        q1_symbols.append(i['symbol'])
    print(q1_symbols)
    return 0
#print(q1_sorted)
#-------------------------------------------------------

#Answer for Q2
def q2_answer(is_for_q4=False):
    q2_sorted = list()
    global q2_symbols
    q2_symbols = list()
    for i in transaction_list:
        if i['symbol'][-4:] == 'USDT':
            q2_sorted.append(i)

    q2_sorted = sorted(q2_sorted, key=lambda k: k['count'], reverse=True)

    for i in q2_sorted[:5]:
        q2_symbols.append(i['symbol'])

    if is_for_q4:
        pass
    else:
        print('Answers for Q2:')
        print(q2_symbols)
    return 0
#print(q2_sorted)
#-------------------------------------------------------

#Answer for Q3
def q3_answer():
    q3_total_values = list()
    limit = 200
    #q1_symbols = q1_answer()
    for symbol in q1_symbols:
        q3_link = 'https://binance.com/api/v3/depth?symbol={}&limit={}'.format(symbol, limit)
        q3_request = r.request('GET', q3_link)
        q3_binance_data = json.loads(q3_request.text)

        bids = 0.0
        asks = 0.0
        for bid_i in q3_binance_data['bids']:
            bids += float(bid_i[:][0])
        for ask_i in q3_binance_data['asks']:
            asks += float(ask_i[:][0])
        new = {'symbol':symbol, 'total_bids':round(bids,2), 'total_asks':round(asks,2)}
        q3_total_values.append(new)
    print('Answers for Q3:')
    print(q3_total_values)
    return 0
#-------------------------------------------------------

#Answer for Q4
def q4_answer(is_for_q5=False):

    if is_for_q5:
        q2_answer(True)
    else:
        print('Answers for Q4:')

    global q4_symbol_prices
    q4_symbol_prices = list()
    for symbol in q2_symbols:
        q4_link = 'https://binance.com/api/v3/ticker/price?symbol={}'.format(symbol)
        q4_request = r.request('GET', q4_link)
        q4_binance_data = json.loads(q4_request.text)

        new = {'symbol':q4_binance_data['symbol'], 'price':round(float(q4_binance_data['price']),2)}
        q4_symbol_prices.append(new)

    if is_for_q5:
        q2_answer(True)
    else:
        print(q4_symbol_prices)
    return 0
#-------------------------------------------------------


#Answer for Q5
def q5_answer(is_for_q6=False):
    if is_for_q6:
        pass
    else:
        print('Answers for Q5:')
    delta = list()
    while True:
        #Call Q4
        q4_answer(True)
        try:
        #Calc delta from prev value
            for i in q4_symbol_prices:
                #i['delta'] = 0
                for j in q5_symbol_prices:
                    if i['symbol'] == j['symbol']:
                        i['delta'] = round(abs(i['price'] - j['price']), 2)
        except:
            #Cathes except at first step when q5 symbol is not exist
            pass
        q5_symbol_prices = q4_symbol_prices
        time.sleep(10)
        print(q4_symbol_prices)
        #break

    return 0
#-------------------------------------------------------

# Collector for Q6
class CustomCollector(object):
    def __init__(self):
        pass

    def collect(self):
        q5_answer(True)
        value1 = GaugeMetricFamily("BINANCE_DATA", 'Help text', labels='value')
        for i in q4_symbol_prices:
            value1.add_metric(i['symbol'], i['price'])#, i['delta'])
        yield value1

#Answer for Q6
# - Counters, Gauges, Histograms, Summary
def q6_answer():
    #q5_answer()
    print('Answers for Q6: check http://localhost:8080/metrics')
    if __name__ == '__main__':
        start_http_server(8080)  ## port where metrics need to be exposed.
        REGISTRY.register(CustomCollector())
        while True:
            time.sleep(2)  ## to collect the metrics every 10 sec
    return 0
#-------------------------------------------------------

q1_answer()
q2_answer()
q3_answer()
q4_answer()
q5_answer()
#q6_answer()