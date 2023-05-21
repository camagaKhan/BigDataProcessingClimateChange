import mysql.connector

#pip install mysql-connector-python

# my SQL configuration details
sqlConfiguration = {
    'user':'user1', 
    'password':'P@ss123!', 
    'host':'localhost', 
    'database':'climatechange',
    'connect_timeout': 400  # Adjust the timeout value as needed
}

climatechange = mysql.connector.connect(
  **sqlConfiguration
)


cursor = climatechange.cursor()

sql = "DELETE FROM C02ML"
cursor.execute(sql)

sql = "DELETE FROM climate_change"
cursor.execute(sql)

sql = "DELETE FROM temperature"
cursor.execute(sql)

sql = "DELETE FROM C02"
cursor.execute(sql)

sql = "DELETE FROM ghg_data"
cursor.execute(sql)

climatechange.commit()
