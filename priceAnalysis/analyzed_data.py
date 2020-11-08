import pandas as pd
from database_connect import get_engine
from datetime import timedelta
from priceAnalysis.price_functions import parse_df, price_delta_in_pct, time_to_strike_df
from tqdm import tqdm


def main():
	_df, engine = get_df()
	_df['myDateTimeDirty'] = _df.index
	_df.index = datetime_index_clean(_df.index)
	my_dict = parse_df(_df, engine)
	df_out = pd.DataFrame.from_dict(my_dict, orient='index')
	return df_out, _df


def datetime_index_clean(_index):
	out_dt_list = []
	for i in _index:
		discard = timedelta(minutes=i.minute % 10,
		                    seconds=i.second,
		                    microseconds=i.microsecond)
		i -= discard
		if discard >= timedelta(minutes=5):
			i += timedelta(minutes=10)
		out_dt_list.append(i)
	return out_dt_list


def get_df():
	engine = get_engine()
	sql = r"SELECT DISTINCT myDateTime, ticker FROM schema_name2.raw_data"
	_df = pd.read_sql(sql=sql, con=engine)
	_df = _df.set_index('myDateTime')
	return _df, engine


def time_d_and_strike_d():
	i = 0
	engine = get_engine()
	sql = r"SELECT * FROM schema_name2.opt_price_clean"
	df = pd.read_sql(sql=sql, con=engine, chunksize=250000, index_col='uid')
	with open(r'C:\DATA\working_docs\strike_and_exp_data.csv', 'a') as f:
		for batch in tqdm(df):
			# process the local dataframe
			# each batch is a 10000 row DF of my data
			# batch['timeUntilExpiration'] = time_to_strike(batch['expiration'], batch['myDateTime'])
			batch = time_to_strike_df(batch)
			batch['pctDiffFromStrike'] = price_delta_in_pct(batch['strike'], batch['currentPrice'])
			out = batch[['pctDiffFromStrike', 'timeUntilExpiration']]
			if i == 0:
				out.to_csv(f, mode='a', line_terminator='\n')
			else:
				out.to_csv(f, mode='a', header=False, line_terminator='\n')
			# Fuck everything below, lets just make a big ass CSV

			# # PUSH TO POSTGRES (SAME NAME AS DF)
			# clean_df.to_sql(name="clean_df", con=engine, if_exists="replace", index=False)
			#
			# # push dataframe to db
			# with engine.begin() as conn:
			# 	sql = "CREATE INDEX options_processed_df_id ON clean_df(id)"
			# 	conn.execute(sql)
			#
			# 	sql = """UPDATE schema_name2.opt_price_clean t
			#              SET t.clean_int_1 = c.int1,
			#                  t.clean_int_2 = c.int2,
			#                  t.updated_date = GETDATE()
			#              FROM clean_df c
			#              WHERE c.id = t.id
			#           """
			# 	conn.execute(sql)
			#
			# 	sql = "DROP TABLE IF EXISTS clean_df"
			# 	conn.execute(sql)

			# engine.dispose()
			i += 1
	return None


if __name__ == '__main__':
	# x, df = main()
	# x.to_csv(r"C:\DATA\working_docs\x.csv")
	z = time_d_and_strike_d()
