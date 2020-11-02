import pymongo
import sys
from bson.json_util import dumps, RELAXED_JSON_OPTIONS
from tqdm import tqdm

client = pymongo.MongoClient(
	"mongodb+srv://admin1:admin@cluster0.fm12j.mongodb.net/<dbname>?retryWrites=true&w=majority")
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
print(f'succesfully dropped \'{db.name}\' database ')
