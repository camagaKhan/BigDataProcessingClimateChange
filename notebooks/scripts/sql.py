import mysql.connector
import re
import numpy as np

import pandas as pd

sqlConfiguration = {
    'user':'user1', 
    'password':'P@ss123!', 
    'host':'localhost', 
    'database':'climatechange',
    'connect_timeout': 400  # Adjust the timeout value as needed
}

dataset = pd.read_csv('/media/camagakhan/DATA/Repositories/BigDataProcessing/Assignment/BigDataProcessingClimateChange/Data/temperature.csv')

print()

values = list(dataset.loc[14791])


def __query__(table_name, columns):
        '''
        Creates an insert statement 
        '''
        column_count = len(columns)
        values, col_names = ', '.join([ "%s" for _ in range(column_count)]), ', '.join([ re.sub(r'^\ufeff', '', col).replace('"', '') for col in columns])
        return """INSERT INTO {table} ({columns}) VALUES ({values});""".format(table=table_name, columns=col_names, values=values)


myQuery = __query__('temperature', dataset.columns)
print(myQuery)

conn = mysql.connector.connect(**sqlConfiguration)
cursor = conn.cursor()
for i in range(len(values)):
    if isinstance(values[i], np.int64):
        values[i] = int(values[i])
cursor.execute(myQuery, tuple(values))
cursor.close()
conn.commit()