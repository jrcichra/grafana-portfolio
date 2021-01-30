import pymysql.cursors
import yfinance as yf
import yaml
import inotify.adapters
import threading
import time
import re
import dateparser
import datetime


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
        ret = None
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(stmt)
                ret = self.connection.insert_id()
                self.connection.commit()
        except Exception as e:
            print(f"Failed to execute: {stmt}")
            print(e)
        return ret

    def create_tables(self):
        self.sql("CREATE DATABASE stocks")
        for ticker in self.tickers.keys():
            ticker = ticker.lower()
            tick = f"""
                CREATE TABLE stocks.tickers (
                    id bigint primary key auto_increment,
                    `date` DATETIME not null,
                    ticker varchar(10) not null,
                    open float,
                    high float,
                    low float,
                    close float,
                    adjclose float,
                    volume float
                )
            """
            self.sql(tick)
            index1 = f"""
                CREATE INDEX stocks_tickers_date_idx
                ON stocks.tickers (`date`)
            """
            self.sql(index1)
            index2 = f"""
                CREATE INDEX stocks_tickers_ticker_idx
                ON stocks.tickers (`ticker`)
            """
            self.sql(index2)
            acc = f"""
                CREATE TABLE stocks.accounts (
                    id bigint primary key auto_increment,
                    name varchar(255) not null
                )
            """
            self.sql(acc)
            lots = f"""
                CREATE TABLE stocks.lots (
                    id bigint primary key auto_increment,
                    account_id bigint not null,
                    ticker varchar(10) not null,
                    `date` datetime not null,
                    price float not null,
                    shares float not null,
                    foreign key (account_id)
                    references stocks.accounts(id)
                )
            """
            self.sql(lots)

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

        # clear out all portfolio data
        self.sql("delete from stocks.accounts")
        self.sql("delete from stocks.lots")

        # gather historical data for all tickers in portfolio
        print(f"Gathering data for {','.join(list(self.tickers.keys()))}...")
        data = yf.download(tickers=list(self.tickers.keys()),
                           period="7d", group_by="ticker", interval="1m")
        # each day
        for date, row in data.iterrows():
            # remove timezone if it has it
            date = re.sub(r'-\d+:\d+$', '', str(date))
            for ticker, row2 in row.groupby(level=0):
                metrics = row2[ticker]
                if str(metrics['Open']) == "nan" or str(metrics['High']) == "nan" or str(metrics['Low']) == "nan" or str(metrics['Close']) == "nan" or str(metrics['Adj Close']) == "nan" or str(metrics['Volume']) == "nan":
                    continue
                insert = f"INSERT INTO stocks.tickers (`date`,ticker,open,high,low,close,adjclose,volume) VALUES ('{date}','{ticker.lower()}',{metrics['Open']},{metrics['High']},{metrics['Low']},{metrics['Close']},{metrics['Adj Close']},{metrics['Volume']})"
                # print(insert)
                self.sql(insert)
        # process user's portfolio
        for account in self.portfolio["accounts"]:
            account_id = self.sql(
                f"INSERT INTO stocks.accounts(name) VALUES ('{account}')")
            for ticker in self.portfolio["accounts"][account].keys():
                # loop through ticker's lots
                for lot in self.portfolio["accounts"][account][ticker]:
                    date = dateparser.parse(lot["date"]).strftime("%Y-%m-%d")
                    price = lot["price"]
                    shares = lot["shares"]
                    self.sql(
                        f"INSERT INTO stocks.lots (account_id,ticker,`date`,price,shares) VALUES ({account_id},'{ticker}','{date}','{price}','{shares}')")
        print(f"Finished populating data")


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
    p.create_tables()
    p.populate()

    # cron thread
    threading.Thread(target=hourly_populate, args=(p, 1)).start()
    # inotify thread
    threading.Thread(target=inotify_populate, args=(p,)).start()
