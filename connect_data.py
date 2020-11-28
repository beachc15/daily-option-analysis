import pymongo
import json
from bson.json_util import dumps
from get_key_from_json import get_key


client = pymongo.MongoClient(get_key())
db = client.misc
collection = (db['End of Day Prices'])
cursor = collection.find({})
all = {}
i = 0
l = list(cursor)
x = dumps(l)

with open('all_prices.json', 'w') as f:
    f.write(x)

