from json import loads


def get_key(key_loc='my_api_key.json', selection='mongo'):
	"""
	Get the api key string for your database and keep it hidden on your public repo. Can upload as well if you can
	encrypt the file.\n
	** Requirements **
		- Make a json file with your api keys saved under the format shown below:
			'{"mongo_api_key" = "<your_api_key_as_str>"}'
		- function will look up a file based on the "key_loc" parameter and, in the file, look for the entry starting
		  with the selection entered (by default: mongo) followed by "_api_key"
	:parameter key_loc: location of json file containing api keys
	:parameter selection: choice of type of api key (in my case either 'mongo' for mongoDB or 'local' for MSSQL)
	:return: connection string of parameters to use in connection engine
	:rtype: str
	:Example:

	>>>from get_key_from_json import get_key
	>>>import pymongo
	>>>
	>>>con_str = get_key(key_loc='my_api_key.json', selection='mongo')
	>>>client = pymongo.MongoClient(mongo_api_key)
	>>> # use client as pymongo object
	"""
	lookup_str = f'{selection}_api_key'
	try:
		with open(key_loc) as f:
			my_secret = loads(f.read())
			mongo_api_key = my_secret[lookup_str]
	except KeyError:
		raise KeyError(f"\'{selection}_api_key\' was not found in {key_loc}. Please update selection from \'{selection}\'")
	except FileNotFoundError:
		raise FileNotFoundError(f"File not found with name \'{key_loc}\'")
	return mongo_api_key
