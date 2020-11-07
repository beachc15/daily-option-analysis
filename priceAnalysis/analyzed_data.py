import pandas as pd
from database_connect import get_engine
from datetime import timedelta
from priceAnalysis.price_functions import parse_df


def main():
	_df, engine = get_df()
	_df.index = datetime_index_clean(_df.index)
	x, y, z = parse_df(_df, engine)
	return x, y, z, _df


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
	sql = r"SELECT DISTINCT TOP 500 myDateTime, ticker FROM schema_name2.raw_data WHERE myDateTime < '2020-09-17' AND " \
	      r"myDateTime > '2020-08-15' "
	_df = pd.read_sql(sql=sql, con=engine)
	_df = _df.set_index('myDateTime')
	return _df, engine


if __name__ == '__main__':
	x, y, z, df = main()
