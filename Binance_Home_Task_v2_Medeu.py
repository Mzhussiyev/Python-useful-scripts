from functools import reduce
from operator import itemgetter
from typing import Any, Dict, List
from time import sleep

from requests_toolbelt.sessions import BaseUrlSession
from rich.table import Table
from rich.console import Console
from prometheus_client import start_http_server, Gauge


def q1(session: BaseUrlSession) -> List[Dict[str, Any]]:
    """Answers for Q1. Print the top 5 symbols with quote asset BTC and the highest volume over the last 24 hours in descending order
    """
    response = session.get("/api/v3/ticker/24hr")
    if response.status_code == 200:
        data = response.json()
        data = sorted(
            filter(lambda d: d.get("symbol").endswith("BTC"), data),
            key=lambda d: float(d.get("quoteVolume")),
            reverse=True
        )
        return data[:5]
    return []


def q2(session: BaseUrlSession) -> List[Dict[str, Any]]:
    """Answers for Q2. Print the top 5 symbols with quote asset USDT and the highest number of trades over the last 24 hours in descending order
    """
    response = session.get("/api/v3/ticker/24hr")
    if response.status_code == 200:
        data = response.json()
        data = sorted(
            filter(lambda d: d.get("symbol").endswith("USDT"), data),
            key=itemgetter("count"),
            reverse=True
        )
        return data[:5]
    return []


def get_order_book(session: BaseUrlSession, symbol: str, limit: int = 500) -> Dict[str, Any]:
    response = session.get("/api/v3/depth", params={"symbol": symbol, "limit": limit})
    if response.status_code == 200:
        return response.json()
    return {}


def q3(session: BaseUrlSession, symbols: List[str]) -> List[Dict[str, Any]]:
    """Answers for Q3. Using the symbols from Q1, what is the total notional value of the top 200 bids and asks currently on each order book?
    """

    order_books = [get_order_book(session, symbol) for symbol in symbols]
    total_notional_values = [
        {
            "symbol": symbol,
            "bids": reduce(
                lambda sum, bid: sum + (float(bid[0]) * float(bid[1])),
                order_book.get("bids")[:200],
                0
            ),
            "asks": reduce(
                lambda sum, ask: sum + (float(ask[0]) * float(ask[1])),
                order_book.get("asks")[:200],
                0
            )
        }
        for symbol, order_book in zip(symbols, order_books)
    ]

    return total_notional_values


def q4(session: BaseUrlSession, symbols: List[str]) -> List[Dict[str, Any]]:
    """Answers for Q4. What is the price spread for each of the symbols from Q2?
    """
    order_books = [get_order_book(session, symbol, limit=100) for symbol in symbols]
    price_spreads = [
        {
            "symbol": symbol,
            "price_spread": max(map(lambda item: float(item[0]), order_book.get("asks"))) -
                            min(map(lambda item: float(item[0]), order_book.get("bids")))
        }
        for symbol, order_book in zip(symbols, order_books)
    ]
    return price_spreads


def q5(session: BaseUrlSession, symbols: List[str]) -> List[Dict[str, Any]]:
    """Answers for Q5. Every 10 seconds print the result of Q4 and the absolute delta from the previous value for each symbol
    """

    current_price_spreads = [{**item, "delta": 0} for item in q4(session, symbols)]
    while True:
        yield current_price_spreads
        prev_price_sreads = current_price_spreads.copy()
        current_price_spreads = [
            {
                **curr_item,
                "delta": abs(curr_item.get("price_spread") - prev_item.get("price_spread"))
            }
            for curr_item, prev_item in zip(q4(session, symbols), prev_price_sreads)
        ]
        sleep(10)

def q6(session: BaseUrlSession) -> List[Dict[str, Any]]:
    """Answers for Q6. Make the output of Q5 accessible by querying http://localhost:8080 using the Prometheus Metrics format
    """

    price_spreads_list = q5(session, [item.get("symbol") for item in q2(session)])
    symbol_gauges = {}
    for item in price_spreads_list:
        for symbol in item:
            #print(symbol)
            symbol_gauges[symbol['symbol']] = {}
            symbol_gauges[symbol['symbol']]['price_spread_gauge'] = Gauge(symbol['symbol'] + '_price_spread_gauge', symbol['symbol'] + ' Price Spread')
            symbol_gauges[symbol['symbol']]['abs_delta_gauge'] = Gauge(symbol['symbol'] + '_abs_delta_gauge', symbol['symbol'] + ' Absolute Delta')
        break

    start_http_server(8080)
    try:
        for price_spreads in price_spreads_list:
            yield price_spreads
            for item in price_spreads:
                symbol_gauges[item['symbol']]['price_spread_gauge'].set(item['price_spread']) #item.get('price_spread'))
                symbol_gauges[item['symbol']]['abs_delta_gauge'].set(item['delta']) #.get('delta'))

    except KeyboardInterrupt:
        exit()



def main():
    session = BaseUrlSession("https://api.binance.com")
    console = Console(width=80)

    q1_answer = q1(session)
    console.print(q1.__doc__.replace("\n", ""), style="bold")
    table = Table("Symbol", "Volume", header_style="bold")
    for item in q1_answer:
        table.add_row(item.get("symbol"), f"{float(item.get('quoteVolume')):.04f}")
    console.print(table)

    q2_answer = q2(session)
    console.print(q2.__doc__.replace("\n", ""), style="bold")
    table = Table("Symbol", "Trade count", header_style="bold")
    for item in q2_answer:
        table.add_row(item.get("symbol"), str(item.get("count")))
    console.print(table)

    q3_answer = q3(session, [item.get("symbol") for item in q1_answer])
    console.print(q3.__doc__.replace("\n", ""), style="bold")
    table = Table("Symbol", "Bids", "Asks", header_style="bold")
    for item in q3_answer:
        table.add_row(item.get("symbol"), f"{item.get('bids'):.04f}", f"{item.get('asks'):.04f}")
    console.print(table)

    q4_answer = q4(session, [item.get("symbol") for item in q2_answer])
    console.print(q4.__doc__.replace("\n", ""), style="bold")
    table = Table("Symbol", "Price spread", header_style="bold")
    for item in q4_answer:
        table.add_row(item.get("symbol"), f"{item.get('price_spread')}")
    console.print(table)

    '''
    console.print(q5.__doc__.replace("\n", ""), style="bold")
    try:
        for price_spreads in q5(session, [item.get("symbol") for item in q2_answer]):
            console.print(datetime.utcnow())
            table = Table("Symbol", "Price Spread", "Delta", header_style="bold")
            for item in price_spreads:
                table.add_row(item.get("symbol"), str(item.get("price_spread")), str(item.get("delta")))
            console.print(table)
    except KeyboardInterrupt:
        exit()
    '''

    console.print(q6.__doc__.replace("\n", ""), style="bold")
    try:
        for price_spreads in q6(session):
            table = Table("Symbol", "Price Spread", "Delta", header_style="bold")
            for item in price_spreads:
                table.add_row(item.get("symbol"), str(item.get("price_spread")), str(item.get("delta")))
            console.print(table)
    except KeyboardInterrupt:
        exit()


if __name__ == "__main__":
    main()
