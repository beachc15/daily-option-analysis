import pandas as pd
import pyodbc
from datetime import date, timedelta, datetime
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


def get_price_history(stock_ticker, current_datetime, timeframe, connection):
	"""returns the historical stock data for the selected time period to do further analysis on including volatility, volume, price trends, etc"""
	# needs to call two separate functions whether the timeframe is small (sql) or large (daily data from yfinance)
	# TODO day trend seems hard to implement at this point but I want to add it. The difficulty comes from how do you
	#  trend early morning results? will it be from the start of the day yesterday? should it be past 3 hours of
	#  records?
	# a) look for date in sql
	# b) if date and range are found
	#   - pull date and range
	# c) else:
	#   - add whatever we can find from yfinance

	# Determine time delta to pass into my SQL lookup function
	if timeframe == '1d':
		time_d = timedelta(days=1)
	elif timeframe == '5d':
		time_d = timedelta(days=5)
	elif timeframe == '1mo':
		# wont be the most consistent thing as i can't really find a months argument but oh well
		time_d = timedelta(weeks=4)
	elif timeframe == '3mo':
		time_d = timedelta(weeks=4 * 3)
	elif timeframe == '6mo':
		time_d = timedelta(weeks=4 * 6)
	elif timeframe == '1y':
		time_d = timedelta(days=365)
	elif timeframe == '2y':
		time_d = timedelta(days=365 * 2)
	elif timeframe == '5y':
		time_d = timedelta(days=365 * 5)
	else:
		raise IndexError(f'Time frame \'{timeframe}\' not found in appropriate list. Please use one of the following options\n'
		                 f'\t[\'1d\', \'5d\', \'1mo\', \'3mo\', \'6mo\', \'1y\', \'2y\', \'5y\']')  # if-else statements for timedelta lookup

	# Determine start date based on current_datetime - timedelta
	print(current_datetime)
	print(timedelta)
	start_date = current_datetime - time_d
	print('Start DT = ', start_date)
	print('End DT = ', current_datetime)

	# TODO update stock ticker to accept the stringed tuple thats in the header row
	def get_price_sql():
		"""Select all for the datetime we are looking at.
		we need to alternate which sql we look up for based on the time_d.
		HARD CODED: cut off date for the start of daily price tracking"""
		cutoff_dt = date(year=2020, month=6, day=26)
		if cutoff_dt > start_date:
			# do not run if the start date is less than the first day we have in the by-the-minute data
			table = "schema_name2.historical_prices_by_day"
			dt_name = "Date"
		else:
			table = "schema_name2.all_prices_to_sql"
			dt_name = "myDateTime"
		curs = connection.cursor()
		# stock_string_for_sql = f'[(\'{sql_diff}\', \'{str(stock_ticker).upper()}\')]'
		sql = f"SELECT * from {table} where {dt_name} = '{start_date}'"
		print(sql)
		result = curs.execute(sql).fetchval()
		curs.close()
		return result

	def get_price_yfinance():
		"""Not used for the purpose of the EFT function lookups. Will be built out for the continuous deployment"""
		# x = yf.Ticker(stock_ticker).history(period=timeframe, end=current_datetime)
		x = 0
		return x
	x = get_price_sql()
	# see if date is in database and if not, add it

	# if the datetime start that we are looking for is not in my repository then lookup from yfinance
	# would this be a switch? I dont want to add 6 if else statements for the different timeframes?
	# probably not...
	return x


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
