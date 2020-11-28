import pymongo
from bson.json_util import dumps, RELAXED_JSON_OPTIONS
from tqdm import tqdm
from get_key_from_json import get_key

mongo_api_key = get_key()

client = pymongo.MongoClient(mongo_api_key)
db = client.data
print(db.name)
count = 0
for month in tqdm(range(0, 13)):
	for date in range(32):
		collection = (db[f'{month}_{date}_2020'])

		cursor = collection.find({})
		all = {}
		i = 0
		l = list(cursor)
		if len(l) >= 1:
			x = dumps(l, json_options=RELAXED_JSON_OPTIONS)
			with open(f'C:/DATA/option_history_repo/stock_data/{month}_{date}_2020.json', 'w') as f:
				f.write(x)
				count += 1
print('****************************')
print()
print(f'Successfully added {count} documents into the \'/data\' directory')
print()
print('*****************************')

for month in tqdm(range(0, 13)):
	for date in range(32):
		collection = (db[f'{month}_{date}_2020'])
		collection.drop()
print(f'successfully dropped \'{db.name}\' database ')
