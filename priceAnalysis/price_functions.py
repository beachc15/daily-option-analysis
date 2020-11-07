import pandas as pd
import pyodbc
from datetime import date, timedelta, datetime
import yfinance as yf
import numpy as np


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


def get_price_history(stock_ticker, current_datetime, connection):
	"""returns the historical stock data for the selected time period to do further analysis on including volatility,
	volume, price trends, etc """

	# needs to call two separate functions whether the timeframe is small (sql) or large (daily data from yfinance)
	# TODO day trend seems hard to implement at this point but I want to add it. The difficulty comes from how do you
	#  trend early morning results? will it be from the start of the day yesterday? should it be past 3 hours of
	#  records?
	# a) look for date in sql
	# b) if date and range are found
	#   - pull date and range
	# c) else:
	#   - add whatever we can find from yfinance

	timeframes = ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y']

	def __histories(timeframe):
		def get_volume_stats(df):
			"""compute volume standard deviation"""
			key = df.columns[-1]  # should be the "Volume" column
			std = np.std(df[key])
			med = np.mean(df[key])
			return std, med

		def get_price_data(df):
			"""compute log return, standard deviation of adj close
			:returns standard deviation, Percent change"""
			key = df.columns[0]  # should be the "Adj Close" column
			std = np.std(df[key])

			# Compute _pct_change
			start = df[key].head(1).values
			end = df[key].tail(1).values
			pct_change = np.log(end / start)
			mean = np.mean(df[key])
			return std, pct_change, mean

		# ----------------
		# DATE HANDLING
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
			raise IndexError(
				f'Time frame \'{timeframe}\' not found in appropriate list. Please use one of the following options\n'
				f'\t[\'1d\', \'5d\', \'1mo\', \'3mo\', \'6mo\', \'1y\', \'2y\', \'5y\']')  # if-else statements for timedelta lookup

		# Determine start date based on current_datetime - timedelta
		start_date = current_datetime - time_d
		# if it's a saturday: round to friday. if it's a sunday: round to monday
		if start_date.weekday() == 5:  # saturday
			start_date = start_date - timedelta(days=1)
		elif start_date.weekday() == 6:  # sunday
			start_date = start_date + timedelta(days=1)

		cutoff_dt = date(year=2020, month=6, day=26)  # This is the first date that is recorded w/ minute data
		if cutoff_dt > start_date:
			# do not run if the start date is less than the first day we have in the by-the-minute data
			table = "schema_name2.historical_prices_by_day"
			dt_name = "myDate"
		else:
			table = "schema_name2.all_prices_to_sql"
			dt_name = "myDateTime"

		# ------------------
		# COLUMN NAME HANDLING
		sql_header_list = ['Adj Close',
		                   'Close',
		                   'High',
		                   'Low',
		                   'Open',
		                   'Volume']
		# add in the ticker name for the sql lookup
		adj_header_list = ['(\'' + c + '\', \'' + stock_ticker.upper() + '\')' for c in sql_header_list]
		col_names = ('\"' + '\", \"'.join(adj_header_list) + '\"')

		# -----------------
		# ACTUAL CALL
		sql_ = f"SELECT {dt_name}, {col_names} from {table} where {dt_name} > '{start_date - timedelta(days=1)}' " \
		       f"AND {dt_name} < '{current_datetime + timedelta(days=1)}' "
		result = pd.read_sql(sql=sql_, con=connection)
		result = result.set_index(result.columns[0])

		_volume, _vol_mean = get_volume_stats(result)
		_price_std, _price_pct_change, _price_mean = get_price_data(result)
		return _volume, _vol_mean, _price_std, _price_pct_change, _price_mean

	out_series = {}
	for tf in timeframes:
		volume, vol_mean, price_std, price_pct_change, price_mean = __histories(tf)
		out_series[tf + '_' + 'vol'] = volume
		out_series[tf + '_' + 'vol_mean'] = vol_mean
		out_series[tf + '_' + 'price_std'] = price_std
		out_series[tf + '_' + 'price_pct_change'] = price_std
		out_series[tf + '_' + 'price_mean'] = price_mean
	return out_series


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
