import pandas as pd
import json


"""In this project I am going to try to add data straight to a SQL table in my database. THIS IDEA HAS BEEN DELAYED"""


with open('C:\DATA\option_history_repo\price_history/all_prices2.json', 'r') as f:
	# r = f.read()
	list_ = json.load(f)

with open('C:\DATA\option_history_repo\price_history/all_prices_to_sql2.csv', 'w') as f:
	for date in list_:
		_id = date['_id']
		this_date = date['date']
		data = pd.read_json(date['data'])
		print(this_date)
		if len(data.index) > 0:
			data.to_csv(path_or_buf=f, line_terminator='\n')
			break
#TODO create a to_sql function that connects to my database



