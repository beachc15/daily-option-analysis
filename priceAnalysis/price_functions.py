import pandas as pd
import pyodbc
from datetime import date
import yfinance as yf


def get_current_price(datetime_, stock_ticker, connection):
	"""For a given datetime and stock ticker, looks up the price of the entered stock ticker at the applicable datetime

	:param datetime_:
	:param connection:
	:param stock_ticker:
	:returns adj_close_price:"""
	sql_diff = 'Adj Close'
	adj_close_price = __lookup_connection(stock_ticker, sql_diff, datetime_, connection)
	return adj_close_price


def get_current_volume(datetime_, stock_ticker, connection):
	"""For a given datetime and stock ticker, looks up the volume of the entered stock ticker at the applicable datetime

	:param datetime_:
	:param connection:
	:param stock_ticker:
	:returns Volume:"""
	sql_diff = 'Volume'
	volume = __lookup_connection(stock_ticker, sql_diff, datetime_, connection)
	return volume


def __lookup_connection(stock_ticker, sql_diff, datetime_, connection):
	"""Takes the sql_diff as the part of the lookup string to change. In this example the two scenarios are either
	'volume' or 'adj close' and does the lookup with an sql string hardcoded inside of the function."""
	curs = connection.cursor()
	stock_ticker = str(stock_ticker).upper()
	stock_string_for_sql = f'[(\'{sql_diff}\', \'{stock_ticker}\')]'
	sql = f"SELECT {stock_string_for_sql} from schema_name2.all_prices_to_sql where myDateTime = '{datetime_}'"
	result = curs.execute(sql).fetchval()
	curs.close()
	return result


def get_price_history(stock_ticker, current_datetime, timeframe, engine):
	# needs to call two separate functions whether the timeframe is small (sql) or large (daily data from yfinance)
	# TODO day trend seems hard to implement at this point but I want to add it. The difficulty comes from how do you
	#  trend early morning results? will it be from the start of the day yesterday? should it be past 3 hours of
	#  records?
	# a) look for date in sql
	# b) if date and range are found
	#   - pull date and range
	# c) else:
	#   - add whatever we can find from yfinance

	def get_price_sql():
		return None

	def get_price_yfinance():
		x = yf.Ticker(stock_ticker).history(period=timeframe, end=current_datetime)
		return x



	# if the datetime start that we are looking for is not in my repository then lookup from yfinance
	# would this be a switch? I dont want to add 6 if else statements for the different timeframes?
	# probably not...
	return get_price_yfinance()


def get_price_volatility_history(timeframe):
	return None


def get_volume_volatility_history(timeframe):
	return None


def get_volatility_func():
	return None


def get_dividends():
	return None


# def get_beta(beta_cache, ticker, date_):
# 	#might get rid of the date for now
# 	# DO NOT USE FEATURE
# 	#       Want to replace with a time respective function taht calculates beta based on prev. financials
# 	lookup_tuple = (str(ticker).upper(), date_)
# 	if lookup_tuple in beta_cache:
# 		r = (beta_cache[lookup_tuple])
# 	else:
# 		beta = yf.Ticker(ticker).get_info()['beta3Year']
# 		r = beta
# 		beta_cache[lookup_tuple] = beta
# 	return r, beta_cache