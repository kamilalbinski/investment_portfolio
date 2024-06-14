import yfinance as yf
import pandas as pd
import logging


def get_tickers_from_assets_df(df, tickers_only=True):
    if not tickers_only:
        tickers = df['YFINANCE_ID', 'ASSET_ID', 'PRICE_MULTIPLIER'].dropna(subset=['YFINANCE_ID'])
    else:
        tickers = df['YFINANCE_ID'].dropna()
    return tickers


def get_tickers_from_fxes_df(df):
    return (df['FROM_CURRENCY'] + df['TO_CURRENCY'] + '=X').to_list()


def get_prices_from_yfinance(tickers, rounding=2):
    results = []
    for ticker in tickers:
        s = yf.download(ticker, period="1d", progress=False)['Close']
        date = str(s.index[0])
        price = round(s.values[0], rounding)
        result = {'YFINANCE_ID': ticker, 'CURRENT_PRICE': price, 'PRICE_DATE': date}
        results.append(result)
    df = pd.DataFrame(results)
    return df


def download_adjusted_prices_from_yfinance(df):
    data_rows = []
    for index, row in df.iterrows():
        ticker = row['YFINANCE_ID']
        date = row['DATE']
        multiplier = row['PRICE_MULTIPLIER']

        start_date = pd.to_datetime(date) + pd.Timedelta(days=1)
        end_date = pd.Timestamp.now()  # Assume current date as the end date

        yfinance_record = yf.download(ticker, start=start_date.strftime('%Y-%m-%d'),
                                      end=end_date.strftime('%Y-%m-%d'),
                                      progress=False)

        if not yfinance_record.empty:
            yfinance_record['Adj Close'] = yfinance_record['Adj Close'] / multiplier
            for stock_date, adj_close in yfinance_record['Adj Close'].items():
                data_rows.append({
                    'ASSET_ID': row['ASSET_ID'],
                    'DATE': stock_date.strftime('%Y-%m-%d 00:00:00'),
                    'PRICE': round(adj_close, 4)
                })
    results_df = pd.concat([pd.DataFrame([row]) for row in data_rows], ignore_index=True)
    return results_df


def get_prices_from_yfinance_2(tickers, rounding=2):
    results = []
    for ticker in tickers:
        s = yf.download(ticker, start=11, end=11, progress=False)
        date = str(s.index[0])
        price = round(s.values[0], rounding)
        result = {'YFINANCE_ID': ticker, 'CURRENT_PRICE': price, 'PRICE_DATE': date}
        results.append(result)
    df = pd.DataFrame(results)
    return df

# data = yf.download(  # or pdr.get_data_yahoo(...
#         # tickers list or string as well
#         tickers = "SPY AAPL MSFT",
#
#         # use "period" instead of start/end
#         # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
#         # (optional, default is '1mo')
#         period = "ytd",
#
#         # fetch data by interval (including intraday if period < 60 days)
#         # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
#         # (optional, default is '1d')
#         interval = "1m",
#
#         # group by ticker (to access via data['SPY'])
#         # (optional, default is 'column')
#         group_by = 'ticker',
#
#         # adjust all OHLC automatically
#         # (optional, default is False)
#         auto_adjust = True,
#
#         # download pre/post regular market hours data
#         # (optional, default is False)
#         prepost = True,
#
#         # use threads for mass downloading? (True/False/Integer)
#         # (optional, default is True)
#         threads = True,
#
#         # proxy URL scheme use use when downloading?
#         # (optional, default is None)
#         proxy = None
#
