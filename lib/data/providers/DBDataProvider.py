import sqlite3
import ccxt

class DBDataProvider(object):
    def __init__(self, exchange, db_file, timeframe, symbols=None):
        # default timeframe 1m
        if symbols is None:
            symbols = ['BTC/USDT', 'ETH/USDT']
        self.conn = sqlite3.connect(db_file)
        self.exchange = exchange
        self.symbols = symbols
        self.timeframe = timeframe

    def initialize(self):
        for symbol in self.symbols:
            tbl = DBDataProvider.table_name(self.exchange, symbol, self.timeframe)
            sql = '''
                create table if not exists {table} (
                    date integer primary key asc,
                    open real,
                    high real,
                    low real,
                    close real,
                    volume real
                )
            '''.format(table=tbl)
            c = self.conn.cursor()
            c.execute(sql)
            self.conn.commit()
        self.update(limit=2000)

    def update(self, limit=50):
        for symbol in self.symbols:
            ex = getattr(ccxt, self.exchange)()
            self.insert_ohlcv(symbol, ex.fetch_ohlcv(symbol, timeframe=self.timeframe, limit=limit))

    def insert_ohlcv(self, symbol, ohlcvs):
        c = self.conn.cursor()
        sql = '''
            replace into {table} (date, open, high, low, close, volume) values 
        '''.format(table=DBDataProvider.table_name(self.exchange, symbol, self.timeframe))
        values = ['(' + ','.join([str(i) for i in x]) + ')' for x in ohlcvs]
        sql += ','.join(values)
        print(sql)
        c.execute(sql)
        self.conn.commit()

    @staticmethod
    def table_name(exchange, symbol, timeframe):
        symbol = symbol.replace('/', '_')
        return f'{exchange}_{symbol}_{timeframe}'

