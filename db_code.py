import sqlite3
import pandas as pd

conn = sqlite3.connect('STAFF.db')

table_name = 'INSTRUCTOR'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

file_path = 'data/INSTRUCTOR.csv'
df = pd.read_csv(file_path, names=attribute_list)

df.to_sql(table_name, conn, if_exists='replace', index=False)
print("Table is ready")

# Now let's run a few queries on the db and print the outputs
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(f"QUERY:\n{query_statement}\n")
print(f"RESPONSE:\n{query_output}\n")

query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(f"QUERY:\n{query_statement}\n")
print(f"RESPONSE:\n{query_output}\n")

query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(f"QUERY:\n{query_statement}\n")
print(f"RESPONSE:\n{query_output}\n")

# Now append a new entry to the database
data_dict = {
    'ID' : [100],
    'FNAME' : ['John'],
    'LNAME' : ['Doe'],
    'CITY' : ['Paris'],
    'CCODE' : ['FR']
}
data_append = pd.DataFrame(data_dict)
data_append.to_sql(table_name, conn, if_exists='append', index=False)
print(f"Data appended successfully\n")

query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(f"QUERY:\n{query_statement}\n")
print(f"RESPONSE:\n{query_output}\n")

conn.close()