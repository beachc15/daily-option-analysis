import pymongo
import json
from bson.json_util import dumps

client = pymongo.MongoClient("mongodb+srv://admin1:admin@cluster0.fm12j.mongodb.net/<dbname>?retryWrites=true&w=majority")
db = client.misc
collection = (db['End of Day Prices'])
cursor = collection.find({})
all = {}
i = 0
l = list(cursor)
x = dumps(l)

with open('all_prices.json', 'w') as f:
    f.write(x)

