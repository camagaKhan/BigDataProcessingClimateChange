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

**Note** I've omitted the database from github

### Configuring SQL Lite 3

```
sudo apt update
sudo apt install sqlite3
sqlite3 --version
```
Output after the three commands should be similar to this

> Output 3.31.1 2020-01-27 19:55:54 3bfa9cc97da10598521b342961df8f5f68c7388fa117345eeb516eaa837balt1

In the project directory go to the ```Data``` folder (cd Data)

Create a Database with this command: 

```
sqlite3 climatechange.db
```

Paste this line near the ```sqlite>```

```
CREATE TABLE emissions (COU text NOT NULL, Country text NOT NULL, POL text NOT NULL, Pollutant text NOT NULL, VAR text NOT NULL, Variable text NOT NULL, YEA integer NOT NULL, Year integer NOT NULL, UnitCode text NULL, Unit text NULL, PowerCodeCode integer NOT NULL, PowerCode text NOT NULL, ReferencePeriodCode real NULL, ReferencePeriod real NULL, Value real NOT NULL, FlagCodes text NULL, Flags text NULL);
```

Since the table is empty run the following code:

```PRAGMA table_info(emissions);```

[You should see something like this](Images/Screenshot.png)


# The Solution

You have a folder called ```Data```, which contains the csv datasets used for this solution. You have AIR_GHG.csv and NASA_SurfaceTemperature.ipynb. These datasets are used. I might use more

In the ```kafka-config``` folder you have the basic configurations to create topics and producers for Apache Kafka

In the notebooks directory, the folder is split in to ```scripts``` (these contain the basic structure of the Stream Processing mechanism). You have the ClimateChange.ipynb and the temperature.ipynb notebooks. In the ClimateChange.ipynb we use the scripts to communicate with Apache Kafka and Apache Spark (using PySpark)
