import pandas as pd
import bson.json_util as bson
from tqdm import tqdm
import os, glob
import re

def parse_symbol(sym, opt):
	''' Takes the contract symbol and deconstructs it to find strike price'''
	if opt == 'call':
		split = 'C'
	else:
		split = 'P'
	y = sym.split(split)
	return float(int(y[-1])/1000)





path = 'C:/DATA/option_history_repo/data'
# TODO create a with open clause in the outer scope to try to reduce memory leak
option_types = ['call', 'put']
big_df = []
filenames = []
with open('C:/DATA/option_history_repo/data/all.csv', 'w') as out_csv:

	for filename in glob.glob(os.path.join(path, '*.json')):
		with open(filename, 'r') as f:
			filenames.append(filename)
	for file in tqdm(filenames):
		with open(f'{file}') as f:
			reader = f.read()
			data = bson.loads(reader)
			reader = ''

		str_date = file.split('\\')[-1].split('.')[0]
		temp_date = str_date
		for timed_data in data:
			temp_id = timed_data['_id']
			temp_time = timed_data['time']
			for ticker in (timed_data['data']):
				for expiration in timed_data['data'][ticker]:
					for option in option_types:
						table = timed_data['data'][ticker][expiration][option]

						df = pd.read_json(table)
						df.insert(loc=0, column='datetime', value=temp_date.replace('_', '-') + ' ' + temp_time)
						df.insert(loc=1, column='id', value=temp_id)
						# df.insert(loc=2, column='time', value=temp_time)
						df.insert(loc=2, column='option_type', value=option[0])
						df.insert(loc=3, column='strike', value=[parse_symbol(x, option) for x in df['contractSymbol']])
						df.insert(loc=4, column='ticker', value=ticker)
						df.insert(loc=5, column='expiration', value=expiration)

						df['uid'] = df['contractSymbol'] + '-' + temp_date + '-' + temp_time
						# check if big DF is empty, then create this as big_df

						df = df.set_index('uid')
						if len(big_df) == 0:
							big_df = df
						else:
							big_df = big_df.append(df)
					big_df.to_csv(out_csv, index_label='uid', header=False)
					big_df = []