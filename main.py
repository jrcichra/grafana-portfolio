import pymysql.cursors
import yfinance as yf
import yaml
import inotify.adapters
import threading
import time


class Portfolio:
    def __init__(self):
        super().__init__()
        self.portfolio_path = "/app/portfolio.yml"

    def get_portfolio_path(self):
        return self.portfolio_path

    def connect_to_database(self, password='test'):
        self.connection = pymysql.connect(host='mariadb',
                                          user='root',
                                          password=password,
                                          cursorclass=pymysql.cursors.DictCursor)

    def sql(self, stmt):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(stmt)
                self.connection.commit()
        except Exception as e:
            print(e)

    def create_ticker_tables(self):
        self.sql("CREATE DATABASE stocks")
        for ticker in self.tickers.keys():
            ticker = ticker.lower()
            create = f"""
                CREATE TABLE stocks.{ticker} (
                    id bigint primary key auto_increment,
                    `date` DATETIME UNIQUE,
                    open float,
                    high float,
                    low float,
                    close float,
                    adjclose float,
                    volume float
                )
            """
            self.sql(create)
            index = f"""
                CREATE INDEX stocks_{ticker}_idx
                ON stocks.{ticker} (`date`)
            """
            self.sql(index)

    def read(self):
        with open(self.portfolio_path, "r") as portfolio_file:
            # get yml
            self.portfolio = yaml.load(portfolio_file, Loader=yaml.FullLoader)
            # generate unique hash of tickers
            self.tickers = {}
            for account in self.portfolio["accounts"]:
                for ticker in self.portfolio["accounts"][account].keys():
                    self.tickers[ticker] = 1

    def populate(self):

        # gather historical data for all tickers in portfolio
        print(f"Gathering data for {','.join(list(self.tickers.keys()))}...")
        data = yf.download(tickers=list(self.tickers.keys()),
                           period="7d", group_by="ticker")
        # each day
        for date, row in data.iterrows():
            for ticker, row2 in row.groupby(level=0):
                metrics = row2[ticker]
                insert = f"INSERT INTO stocks.{ticker.lower()} (`date`,open,high,low,close,adjclose,volume) VALUES ('{date}',{metrics['Open']},{metrics['High']},{metrics['Low']},{metrics['Close']},{metrics['Adj Close']},{metrics['Volume']})"
                print(insert)
                self.sql(insert)


def hourly_populate(p, hours):
    time.sleep(60 * 60 * hours)
    print(f"Cron is populating...")
    p.populate()


def inotify_populate(p):
    while True:
        i = inotify.adapters.Inotify()
        i.add_watch(p.get_portfolio_path())
        for event in i.event_gen(yield_nones=False):
            (_, type_names, _, _) = event
            if type_names == "IN_MODIFY":
                print(f"inotify is populating...")
                p.populate()


if __name__ == "__main__":
    p = Portfolio()
    p.read()
    p.connect_to_database()
    p.create_ticker_tables()
    p.populate()

    # cron thread
    threading.Thread(target=hourly_populate, args=(p, 1)).start()
    # inotify thread
    threading.Thread(target=inotify_populate, args=(p,)).start()
