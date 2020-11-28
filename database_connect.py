import pyodbc
from get_key_from_json import get_key


def get_engine():
	conn = pyodbc.connect(
		get_key(selection='local')
	)
	return conn
