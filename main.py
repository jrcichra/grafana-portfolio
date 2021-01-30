import pymysql.cursors
import yfinance as yf
import yaml


class Portfolio:
    def __init__(self):
        super().__init__()

    def connect_to_database(self, password='test'):
        self.connection = pymysql.connect(host='mariadb',
                                          user='root',
                                          password=password,
                                          database='stocks',
                                          cursorclass=pymysql.cursors.DictCursor)

    def create_ticker_table(self, ticker):
        create = f"""
            CREATE TABLE stocks.{ticker.lower()} (
                `date` DATETIME,
                open float,
                high float,
                low float,
                close float,
                adjclose float,
                volume float
            )
        """
        with self.connection.cursor() as cursor:
            cursor.execute(create)
            self.connection.commit()

    def read(self):
        with open("/app/portfolio.yml", "r") as portfolio_file:
            # get yml
            self.portfolio = yaml.load(portfolio_file, Loader=yaml.FullLoader)

    def populate(self):
        # generate unique hash of tickers
        tickers = {}
        for account in self.portfolio["accounts"]:
            for ticker in self.portfolio["accounts"][account].keys():
                tickers[ticker] = 1

        # gather historical data for all tickers in portfolio
        print(f"Gathering data for {','.join(list(tickers.keys()))}...")
        data = yf.download(tickers=list(tickers.keys()),
                           period="7d", group_by="ticker")
        # each day
        for date, row in data.iterrows():
            for ticker, row2 in row.groupby(level=0):
                # print(f"a={a}")
                # print(row2[ticker]["Open"])
                metrics = row2[ticker]
                insert = f"INSERT INTO stocks.{ticker.lower()} (`date`,open,high,low,close,adjclose,volume) VALUES ('{date}',{metrics['Open']},{metrics['High']},{metrics['Low']},{metrics['Close']},{metrics['Adj Close']},{metrics['Volume']})"
                print(insert)


if __name__ == "__main__":
    p = Portfolio()
    p.read()
    p.populate()
