import findspark as ff
from pathlib import Path
username = 'camagakhan' # change to your username.
ff.init('/home/{0}/spark-3.4.0-bin-hadoop3'.format(username)) # this is the directory I installed spark. If you follow the steps on the readme file, I've highlighted how you can get this directory on Ubuntu. I use this instead of the bashrc command
from pyspark.sql import SparkSession # don't mind the could not be resolved warning. The findspark package automatically locates the pyspark library from the directory you gave it. 
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, FloatType, DoubleType,  IntegerType # we will need to create a schema to save the streamed data in an sqlite3 table

import os
# you'll get an error without the following line. Refer to stack overflow: https://stackoverflow.com/questions/70725346/failed-to-find-data-source-please-deploy-the-application-as-per-the-deployment
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

print('PySpark found') # this will print a simple log after findspark locates the Spark installation on your workstation

print('Does Data Folder exist?', Path('Data').is_dir())
PATH = str(Path.cwd()).replace('notebooks', 'Data') # sorry about this, but Path.cwd() wasn't giving me the project directory, so I had to change it.

'''
    Class is used to establish connection with Apache Kafka and will allow developers to stream data from topics
    It uses an instance called SparkSession. This page provides an explanation about the pyspark.sql.SparkSession. Click on the link:
    https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.SparkSession.html
'''

class Stream_Data(object) :

    def __init__(self, topics:str, host: str) -> None:
        '''
        If multiple hosts are assigned, use this notation: host1:port,host2:port,hostn:port
        @topic: the topic you wish to subscribe to
        @host: the kafka cluster/instance
        '''
        self.topics = topics
        self.host = host

    '''
        function creates a spark session that will enable users to perform streaming techniques.
        Don't call this function directly. Use the function called subscribe.
        @return: returns a SparkSession, so you can then create the DataFrame representing the topic
    '''
    def __getSparkSession__(self):
        return SparkSession \
    .builder \
    .appName('Assignment') \
    .config('spark.sql.debug.maxToStringFields', '100') \
    .getOrCreate()
    

    def __subsribe__(self):
        '''
            get the data after you subscribe to a Apache kafka producer. 
            @return the dataframe of the subscribed topic
        '''
        spark = self.__getSparkSession__()
        dataframe = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", self.host) \
        .option("subscribe", self.topics) \
        .option("startingOffsets", "earliest") \
        .load()

        dataframe = dataframe.withColumn('value', col('value').cast('string'))

        # Apply the schema to the DataFrame
        dataframe = dataframe.select(from_json('value', self.__getEmissionsSchema__()).alias('data')).select('data.*')
        dataframe = dataframe.withColumn('Value', col('Value').cast(DoubleType()))
        dataframe = dataframe.withColumn('Year', col('Year').cast(IntegerType()))
        dataframe = dataframe.withColumn('YEA', col('YEA').cast(IntegerType()))
        return dataframe
    
    def __getEmissionsSchema__(self):
        '''
        Represents the schema of the emissions dataset found in the Data folder
        '''
        schema = StructType().add('\ufeff"COU"', StringType())\
        .add('Country', StringType()) \
        .add('POL', StringType()) \
        .add('Pollutant', StringType()) \
        .add('VAR', StringType()) \
        .add('Variable', StringType()) \
        .add('YEA', StringType()) \
        .add('Year', StringType()) \
        .add('UnitCode', StringType()) \
        .add('Unit', StringType()) \
        .add('PowerCodeCode', StringType())\
        .add('PowerCode', StringType()) \
        .add('ReferencePeriodCode', FloatType()) \
        .add('ReferencePeriod', FloatType()) \
        .add('Value', StringType()) \
        .add('FlagCodes', StringType()) \
        .add('Flags', StringType())

        return schema
    
    def getData(self) :
        df = self.__subsribe__() # subscribe to the producer
        for col in df.dtypes:
            print(col[0]+" , "+col[1])
        #topic = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        db_url = 'jdbc:sqlite:'

        db_path = os.path.join(PATH, 'climatechange.db')
        full_path = db_url + db_path #os.path.join(db_url, db_path)
        print('full path',full_path)

        if os.path.exists(db_path):
            print('database directory....', db_path)

            query = df.writeStream \
                    .format('console').start()

            # query = df.writeStream \
            #         .format('jdbc') \
            #         .option('url', full_path) \
            #         .option('dbtable', 'emissions') \
            #         .start()
            
            query.awaitTermination()
            return query