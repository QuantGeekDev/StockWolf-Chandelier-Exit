import os
import datetime
import pandas as pd
import dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
import requests
import schedule
import time

dotenv.load_dotenv()
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL")
TIME_INTERVAL = os.getenv("TIME_INTERVAL")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")


def get_watchlist():
    """ Converts watchlist.csv to pandas dataframe """
    watchlist_name = "./watchlist.csv"
    print(f"Reading {watchlist_name}...")
    tickers_watchlist = pd.read_csv(watchlist_name)
    return tickers_watchlist


def get_historical_data_start_date(days=30):
    """ Gets start date n days back from today """
    start_date = datetime.date.today() - datetime.timedelta(days=days)
    return start_date


def get_historical_data_timeframe(time_interval="1Day"):
    """ Return Alpaca TimeFrame object based on time_interval """
    if time_interval == "1Day":
        return TimeFrame.Day
    else:
        print("Invalid time interval in .env")


def get_historical_data(watchlist):
    """ Fetches historical data for given tickers and returns dictionary of Dataframes"""
    tickers_historical_data = {}
    data_client = StockHistoricalDataClient(api_key=ALPACA_API_KEY, secret_key=ALPACA_SECRET_KEY)
    start_date = get_historical_data_start_date()
    timeframe = get_historical_data_timeframe()
    for current_ticker, _sell_qty in watchlist.values:
        print(f"Fetching historical data for {current_ticker} ")
        try:
            request_params = StockBarsRequest(
                symbol_or_symbols=current_ticker,
                timeframe=timeframe,
                start=start_date,
                extended_hours=True,
            )
            tickers_historical_data[current_ticker] = data_client.get_stock_bars(request_params).df
        except Exception as e:
            print(f"Failed to get historical data for {current_ticker}: {e}")
            continue

    return tickers_historical_data


def calculate_average_true_range(ticker_df):
    """Calculates the Average True Range (ATR) and appends it to the DataFrame"""
    atr_range = 14
    high_low = ticker_df['high'] - ticker_df['low']
    high_close = (ticker_df['high'] - ticker_df['close'].shift()).abs()
    low_close = (ticker_df['low'] - ticker_df['close'].shift()).abs()

    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = ranges.max(axis=1)

    average_true_range = true_range.ewm(alpha=1/atr_range, adjust=False).mean()

    ticker_df['average_true_range'] = average_true_range

    return ticker_df


def calculate_highest_price(ticker_df, lookback_period=22):
    """ Returns the highest price within the last lookback_period rows """
    if len(ticker_df) >= lookback_period:
        highest_price = ticker_df["high"][-lookback_period:].max()
    else:
        highest_price = ticker_df["high"].max()
    return highest_price


def calculate_chandelier_exit(average_true_range, highest_price, multiplier=2.5):
    """ Calculates chandelier exit price following this formula:
    Chandelier Exit = Highest High – (ATR * Multiplier)"""
    chandlier_exit_price = highest_price - (average_true_range * multiplier)
    return chandlier_exit_price


def notify_telegram_channel(report):
    """ Sends the report to a selected Telegram Channel"""
    message = f"New Chandelier Exit report has been generated: \n {report}"
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage?chat_id={TELEGRAM_CHAT_ID}&text={message}"
    requests.post(url)


def generate_chandelier_exit_report(tickers_historical_data):
    """ Generate a chandelier exit price report as a string """
    report = ""
    for ticker, data in tickers_historical_data.items():
        try:
            tickers_historical_data[ticker] = calculate_average_true_range(data)
            highest_price = calculate_highest_price(data)
            current_average_true_range = data.tail(1)["average_true_range"].values[0]
            current_chandelier_exit = round(calculate_chandelier_exit(current_average_true_range, highest_price), 2)
            ticker_report = f"\n{ticker}: Chandelier Exit Price = ${current_chandelier_exit} "
            print(ticker_report)
            report = report + ticker_report
        except Exception as e:
            print(f"Unable to calculate Chandelier Exit for {ticker} : {e}")
            continue
    return report


def job():
    """ Main analysis logic loop"""
    tickers = get_watchlist()
    tickers_historical_data = get_historical_data(tickers)
    report = generate_chandelier_exit_report(tickers_historical_data)
    notify_telegram_channel(report)


if __name__ == "__main__":
    print("Initializing 🐺StockWolf: Chandelier Exit\n")
    schedule.every().day.at("09:30", "America/New_York").do(job)
    while True:
        schedule.run_pending()
        time.sleep(600)
        print("Waiting until 9:30AM New York time")
