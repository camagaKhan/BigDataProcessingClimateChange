# BigDataProcessingClimateChange

Hi, for this assignment we'll be using **Apache Kafka** and **Apache Spark**. This readme file will be updated as the solution improves and grows. This solution is tested and produced using Ubuntu

> v: Ubuntu 22.04.2; LTS : Release: 22.04; Codename: jammy 

### What you will need to run this solution: 


```
pip install kafka-python
pip3 install findspark
pip install mysql-connector-python

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

+ I've included the jar files for ```graphframes``` 

**Note** I've omitted the database from github

### Configuring mySQL 

https://www.digitalocean.com/community/tutorials/how-to-install-mysql-on-ubuntu-20-04

Always ensure that the mySQL server is on by running this command: sudo systemctl start mysql.service and then this command 'sudo systemctl status mysql.service'
To Stop the server run this command: 'sudo systemctl stop mysql.service'

Log in as root: ```sudo mysql```

It should prompt you to input your log in password

FIRST create a user: 

```CREATE USER 'user1'@'localhost' IDENTIFIED BY 'P@ss123!';```

then run this command (to create the DB):

```CREATE DATABASE climatechange;```

This command grants access to the created user (the one we created earlier on):

```GRANT ALL ON climatechange.* TO 'user1'@'localhost';```

Run the following scripts to generate the tables 

```
CREATE TABLE ghg_data(Country text NULL, Year integer NULL, Value real NULL, Pollutant text NULL);
CREATE TABLE CO2(Entity text NULL, Code text NULL, Year integer NULL, Value real NULL);

```

```
CREATE TABLE temperature (REF_AREA text NULL, Measure text NULL, UNIT_MEASURE text NULL, TIME_PERIOD integer NULL, OBS_VALUE real NULL, REF_CODE text NULL);
```

For the Machine Learning section create these tables.

```
CREATE TABLE climate_change(COUNTRY_CODE text NULL, Country text NULL, Year integer NULL, UNIT_MEASURE text NULL, OBS_VALUE real NULL, POL text NULL, VALUE real NULL);

CREATE TABLE C02ML(COUNTRY_CODE text NULL, Country text NULL, Year integer NULL, UNIT_MEASURE text NULL, OBS_VALUE real NULL, POL text NULL, VALUE real NULL);

```

Since the table is empty run the following code:

```DESCRIBE ghg_data;``` and ```DESCRIBE temperature;```

[You should see something like this](Images/table_details.png)

***Note*** the screenshot shows another table name. The table emissions was renamed to ghg_data. Thanks


# The Solution

You have a folder called ```Data```, which contains the csv datasets used for this solution. You have AIR_GHG.csv and NASA_SurfaceTemperature.ipynb. These datasets are used. I might use more

In the ```kafka-config``` folder you have the basic configurations to create topics and producers for Apache Kafka

In the notebooks directory, the folder is split in to ```scripts``` (these contain the basic structure of the Stream Processing mechanism). You have the ClimateChange.ipynb and the temperature.ipynb notebooks. In the ClimateChange.ipynb we use the scripts to communicate with Apache Kafka and Apache Spark (using PySpark)
