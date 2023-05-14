import findspark as ff
import mysql.connector 
from pathlib import Path
import time
import re
import sys
username = 'camagakhan' # change to your username.
ff.init('/home/{0}/spark-3.4.0-bin-hadoop3'.format(username)) # this is the directory I installed spark. If you follow the steps on the readme file, I've highlighted how you can get this directory on Ubuntu. I use this instead of the bashrc command
from pyspark.sql import SparkSession # don't mind the could not be resolved warning. The findspark package automatically locates the pyspark library from the directory you gave it. 
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, FloatType, DoubleType, IntegerType # we will need to create a schema to save the streamed data in an sqlite3 table
from pyspark import SparkContext
import warnings

import os
# you'll get an error without the following line. Refer to stack overflow: https://stackoverflow.com/questions/70725346/failed-to-find-data-source-please-deploy-the-application-as-per-the-deployment
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
os.environ['SPARK_OPTS'] = '--packages graphframes:graphframes:0.8.2-spark2.4-s_2.11' #SPARK_OPTS="--packages graphframes:graphframes:0.8.2-spark2.4-s_2.11"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

print('PySpark found') # this will print a simple log after findspark locates the Spark installation on your workstation

PATH = str(Path.cwd()).replace('notebooks', 'Data') # sorry about this, but Path.cwd() wasn't giving me the project directory, so I had to change it.

'''
    Class is used to establish connection with Apache Kafka and will allow developers to stream data from topics
    It uses an instance called SparkSession. This page provides an explanation about the pyspark.sql.SparkSession. Click on the link:
    https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.SparkSession.html
'''
warnings.filterwarnings('ignore')
# my SQL configuration details
sqlConfiguration = {
    'user':'user1', 
    'password':'P@ss123!', 
    'host':'localhost', 
    'database':'climatechange',
    'connect_timeout': 400  # Adjust the timeout value as needed
}



class Stream_Data(object) :

    def __init__(self, topics:str, host: str) -> None:
        '''
        If multiple hosts are assigned, use this notation: host1:port,host2:port,hostn:port
        @topic: the topic you wish to subscribe to
        @host: the kafka cluster/instance
        '''
        self.topics = topics
        self.host = host
        #self.sqlConnection = mysql.connector.connect(user='user1', host='localhost', password='P@ss123!', database='climatechange')

    '''
        function creates a spark session that will enable users to perform streaming techniques.
        Don't call this function directly. Use the function called subscribe.
        @return: returns a SparkSession, so you can then create the DataFrame representing the topic
    '''
    def __getSparkSession__(self):
        mySQL_jar = self.__getMySQLDir__() # the directory of the sqlite jar
        graphframes_jar = '{0}/scripts/graphframes-0.8.2-spark2.4-s_2.11.jar'.format(os.getcwd())

        pySparkSession = (SparkSession 
                .builder 
                .appName('Assignment') 
                .config(
                    'spark.jars',
                    mySQL_jar
                )
                .config(
                    'spark.driver.extraClassPath',
                    mySQL_jar
                )
                .getOrCreate())
        
        sc = pySparkSession.sparkContext
        sc.addPyFile(graphframes_jar)
        sc.setLogLevel('ERROR') # warnings we can do without. Just show errors and fatal errors

        return pySparkSession
    


    def __getMySQLDir__(self):
        '''
        @return: function returns the sqlite jar file. This is conventiently stored in this project.
        '''
        directory = '{}/scripts/mysql-connector-j-8.0.33.jar'.format(os.getcwd())
        print(directory)
        return directory
    

    def __subsribe__(self):
        '''
            get the data after you subscribe to a Apache kafka producer. 
            @return the dataframe of the subscribed topic
        '''
        spark = self.__getSparkSession__()
        dataframe = (spark.readStream 
                    .format("kafka") 
                    .option("kafka.bootstrap.servers", self.host) 
                    .option("subscribe", self.topics) 
                    .option("startingOffsets", "earliest") 
                    .load())

        dataframe = dataframe.withColumn('value', col('value').cast('string'))

        if self.topics == 'ghg_data':
            # Apply the schema to the 'emissions' DataFrame
            dataframe = dataframe.select(from_json('value', self.__getEmissionsSchema__()).alias('data')).select('data.*')
            dataframe = dataframe.withColumn('Value', col('Value').cast(DoubleType())) # not to be confused with value which is a Kafka property (the row value encoded in bytes). The Value column here represents the ghg value
            dataframe = dataframe.withColumn('Year', col('Year').cast(IntegerType()))
            dataframe = dataframe.withColumn('YEA', col('YEA').cast(IntegerType()))
        elif self.topics == 'temperature':
            # Apply the schema to the 'temperature' DataFrame
            dataframe = dataframe.select(from_json('value', self.__temperatureSchema__()).alias('data')).select('data.*')
            dataframe = dataframe.withColumn('TIME_PERIOD', col('TIME_PERIOD').cast(IntegerType())) # not to be confused with value which is a Kafka property (the row value encoded in bytes). The Value column here represents the ghg value
            dataframe = dataframe.withColumn('OBS_VALUE', col('OBS_VALUE').cast(DoubleType()))

        return dataframe
    
    def __temperatureSchema__(self):
        '''
        Sets the temperature schema
        @returns the temperature schema used
        '''
        schema = (
            StructType()
            .add('REF_AREA', StringType())
            .add('Measure', StringType())
            .add('UNIT_MEASURE', StringType())
            .add('TIME_PERIOD', StringType())
            .add('OBS_VALUE', StringType())
            .add('REF_CODE', StringType())
        )
        return schema
    
    def __getEmissionsSchema__(self):
        '''
        Represents the schema of the emissions dataset found in the Data folder
        '''
        schema = (StructType().add('\ufeff"COU"', StringType())
        .add('Country', StringType()) 
        .add('POL', StringType()) 
        .add('Pollutant', StringType()) 
        .add('VAR', StringType()) 
        .add('Variable', StringType())
        .add('YEA', StringType())
        .add('Year', StringType())
        .add('UnitCode', StringType())
        .add('Unit', StringType())
        .add('PowerCodeCode', StringType())
        .add('PowerCode', StringType())
        .add('ReferencePeriodCode', FloatType())
        .add('ReferencePeriod', FloatType())
        .add('Value', StringType())
        .add('FlagCodes', StringType())
        .add('Flags', StringType()))

        return schema
    
    def getData(self) :
        df = self.__subsribe__() # subscribe to the producer
        #self.__dataframe__ = df
        query = (df.writeStream 
                .format('console').start())
        
        self.columns = df.columns # we'll need this to map to the table later on

        query = (df.writeStream
                .format('jdbc')
                .foreach(self.__write__)
                .start())
        
        return query, df
    
    def getDataframe(self) :
        return self.__dataframe__
    
    def __query__(self, table_name, columns):
        '''
        Creates an insert statement 
        '''
        column_count = len(columns)
        values, col_names = ', '.join([ "%s" for _ in range(column_count)]), ', '.join([ re.sub(r'^\ufeff', '', col).replace('"', '') for col in columns])
        return """INSERT INTO {table} ({columns}) VALUES ({values});""".format(table=table_name, columns=col_names, values=values)
        
    def __write__(self, row):  
        '''
        Writes the value in the respective table using a MySQL INSERT statement. Once the values of the streaming data are added to the database then
        we commit the values to the SQL table
        '''      
        conn = mysql.connector.connect(**sqlConfiguration)
        table_name ='ghg_data'

        if self.topics == 'temperature':
            table_name = 'temperature'
                   
        values = list()
        for column in self.columns:
            values.append(row[column])
        values = tuple(values)
        insert_qry = self.__query__(table_name=table_name, columns=self.columns)
        cursor = conn.cursor()
        cursor.execute(insert_qry, values)
        cursor.close()
        conn.commit()
