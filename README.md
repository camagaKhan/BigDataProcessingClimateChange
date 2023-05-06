# BigDataProcessingClimateChange

Hi, for this assignment we'll be using **Apache Kafka** and **Apache Spark**. This readme file will be updated as the solution improves and grows. This solution is tested and produced using Ubuntu

> v: Ubuntu 22.04.2; LTS : Release: 22.04; Codename: jammy 

### What you will need to run this solution: 


```
pip install kafka-python
pip3 install findspark

```

I use ```kafka-python``` to create the topics, producers, and consumers
```findspark``` is used to bridge the location of where you installed the spark framework. We'll be using **Python** for this as well =]

Download Software:

+ [Anaconda](https://www.anaconda.com/download)
+ [Apache Kafka](https://kafka.apache.org/downloads)
+ Download the tgz file for [Apache Spark](https://www.apache.org/dyn/closer.lua/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz) using the suggested mirror link.


### Configuring Apache Spark

+ Use these commands to configure spark. (Please install ```findspark``` first)

+ After downloading ```Apache Spark``` rename the ```tgz``` folder to spark.tgz. The folder is in the download folder

+ Open the ```home``` directory in terminal, write ```mkdir spark``` to create a folder. 

+ Write this command: ```tar -xvzf ~/Downloads/spark.tgz --strip 1``` so that the inner files are pasted in the spark folder

+ ```cd spark``` and type ```pwd``` to get the directory. copy it. It should look like this ```/home/[YOUR_USER]/spark```. You will need this to update the sections were I use PySpark