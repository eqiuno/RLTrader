from lib.data.providers import DBDataProvider

import os
import argparse
import logging
import ccxt
import sqlite3

root = os.path.dirname(__file__)

DATA_DIR = os.path.join(root, 'data')
LOG_DIR = os.path.join(root, 'log')
formatter = '%(asctime)s  %(filename)s : %(levelname)s  %(message)s'
console = logging.StreamHandler()
console.setFormatter(formatter)
console.setLevel(logging.DEBUG)
logging.basicConfig(filename=os.path.join(LOG_DIR, 'cron.log'), level=logging.INFO, format=formatter)



def create_table(conn, table_name):
    sql = f'''
        create table if not exists {table_name} (
            date integer primary key asc,
            open real,
            high real,
            low real,
            close real,
            volume real
        )
    '''
    c = conn.cursor()
    c.execute(sql)
    conn.commit()
    logging.info(f'create table [{table_name}]')


def sync(conn, exchange, symbols, interval, limit=200):
    ex = getattr(ccxt, exchange)()
    logging.info(f"sync [{','.join(symbols)}] from {exchange}, limit={limit}...")
    for symbol in symbols:
        table = table_name(exchange, symbol, interval)
        ohlcv_data = ex.fetch_ohlcv(symbol, timeframe=interval, limit=limit)
        logging.info(f'sync [{symbol}] from [{exchange}] got [{len(ohlcv_data)}] rows')
        record(conn, table, ohlcv_data)


def ohlcv_to_db_row(ohlcv):
    return '(' + ','.join([str(f) for f in ohlcv]) + ')'


def table_name(exchange, symbol, inteval):
    symbol = symbol.replace('/', '_')
    return f'{exchange}_{symbol}_{interval}'


def record(conn, table, ohlcv_data):
    sql = f'replace into {table} (date, open, high, low, close, volume) values '
    sql += ','.join([ohlcv_to_db_row(row) for row in ohlcv_data])
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    logging.info(f'record [{len(ohlcv_data)}] rows to [{table}]')


def initialize(exchange):
    logging.info(f'initialize {exchange} data')
    store = DBDataProvider(exchange, DATA_DIR)
    store.initialize()
    logging.info(f'finish initialize {exchange} data')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('exchange', help='exchange name')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-u', '--update', action='store_true')
    group.add_argument('-i', '--initialize', action='store_true')

    args = parser.parse_args()

    db_file = os.path.join(DATA_DIR, f'{args.exchange}.db')

    conn = sqlite3.connect(db_file)
    exchange_name = args.exchange
    interval = '1m'
    symbols = ['BTC/USDT', 'ETH/USDT']

    if args.update:
        sync(conn, exchange_name, symbols, interval, limit=50)

    if args.initialize:
        for symbol in symbols:
            create_table(conn, table_name(exchange_name, symbol, interval))
        sync(conn, exchange_name, symbols, interval, limit=1000)
