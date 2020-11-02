import pyodbc
import pandas as pd
import matplotlib.pyplot as plt


def get_engine():
	conn = pyodbc.connect(
		'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost\SQLEXPRESS;DATABASE=First Table;Trusted_Connection=yes;')
	return conn

# df = pd.read_sql("SELECT * FROM schema_name2.raw_data WHERE ticker like 'spy' and strike like 350 and optType like
# 'p'", conn) df.plot(y='change', x='myDateTime') plt.show()
