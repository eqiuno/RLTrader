from lib.data.providers import DBDataProvider

import os
import argparse
import logging

root = os.path.dirname(__file__)

DATA_DIR = os.path.join(root, 'data')
LOG_DIR = os.path.join(root, 'log')
formatter = '%(asctime)s  %(filename)s : %(levelname)s  %(message)s'
logging.basicConfig(filename=os.path.join(LOG_DIR, 'cron.log'), level=logging.INFO, format=formatter)


def update(exchange):
    logging.info(f'update {exchange} data')
    store = DBDataProvider(exchange, DATA_DIR)
    store.update()
    logging.info(f'finish update {exchange} data')


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

    if args.update:
        update(args.exchange)

    if args.initialize:
        initialize(args.exchange)
