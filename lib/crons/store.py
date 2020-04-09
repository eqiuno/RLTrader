import sqlite3
from string import Template
class Store(object):
    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file)

    def create(self, exchange, symbol, timeframe):
        sql = '''
            create table {table} (
                date integer primary key,
                high real,
                low real,
                open real,
                close real,
                volume real,
                amount real,
                count integer,
                primary key(date)
            )
        '''.format(table=self._table_name(exchange, symbol, timeframe))
        self.conn.execute(sql)

    def insert(self, exchange, symbol, timeframe, records):
        tbl_name = self._table_name(exchange, symbol, timeframe)
        sql = '''
            replace into {table} (date, high, low, open, close, volume, amount, count) values  
        '''.format(table=tbl_name)
        s = Template('($date, $high, $low, $open, $close, $volume, $amount, $count) ')
        for r in records:
            sql += s.substitute(**r)

        self.conn.execute(sql)

    def _table_name(self, exchange, symbol, timeframe):
        return f'{exchange}_{symbol}_{timeframe}'