import findspark as ff
username = 'camagakhan' # change to your username.
ff.init('/home/{0}/spark'.format(username)) # this is the directory I installed spark. If you follow the steps on the readme file, I've highlighted how you can get this directory on Ubuntu
from pyspark.sql import SparkSession # don't mind the could not be resolved warning. The findspark package automatically locates the pyspark library from the directory you gave it. 

print('PySpark found') # this will print a simple log after findspark locates the Spark installation on your workstation

'''
    Class is used to establish connection with Apache Kafka and will allow developers to stream data from topics
    It uses an instance called SparkSession. This page provides an explanation about the pyspark.sql.SparkSession. Click on the link:
    https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.SparkSession.html
'''

class Stream_Data(object) :

    def __init__(self, topic:str, host: str) -> None:
        '''
        If multiple hosts are assigned, use this notation: host1:port,host2:port,hostn:port
        @topic: the topic you wish to subscribe to
        @host: the kafka cluster/instance
        '''
        self.topic = topic
        self.host = host

    '''
        function creates a spark session that will enable users to perform streaming techniques.
        Don't call this function directly. Use the function called subscribe.
        @return: returns a SparkSession, so you can then create the DataFrame representing the topic
    '''
    def __getSparkSession__(self):
        return SparkSession.builder.appName('Assignment').config('spark.sql.debug.maxToStringFields', '100').getOrCreate()
    

    def subsribe(self):
        '''
            get the data after you subscribe to a Apache kafka producer. 
            @return the dataframe of the subscribed topic
        '''
        spark = self.__getSparkSession__()
        dataframe = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", self.host) \
        .option("kafka.security.protocol", "SSL") \
        .option("failOnDataLoss", "false") \
        .option("subscribe", self.topic) \
        .option("includeHeaders", "true") \
        .option("startingOffsets", "latest") \
        .option("spark.streaming.kafka.maxRatePerPartition", "50") \
        .load()

        return dataframe