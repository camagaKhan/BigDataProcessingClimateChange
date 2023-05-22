# BigDataProcessingClimateChange

Hi, for this assignment we'll be using **Apache Kafka** and **Apache Spark**. This readme file will be updated as the solution improves and grows. This solution is tested and produced using Ubuntu

> v: Ubuntu 22.04.2; LTS : Release: 22.04; Codename: jammy 

### What you will need to run this solution: 


```
pip install kafka-python
pip3 install findspark
pip install mysql-connector-python
pip install -U scikit-learn
python -m pip install statsmodels

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
To Stop the server run this command: 'sudo systemctl stop mysql.service'. To Start it: 'sudo systemctl start mysql.service'

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

### PySpark

Download Apache Spark

Go to Apache Spark and go to downloads. Select a mirror link to download it. In the choose package, use Pre-build for Hadoop 2.7 and later. You'll download a .tgz file. Its a large file. Once downloaded move the spark .tgz folder to the home folder. Then open the terminal from home and write.

```
sudo tar -zxvf spark-3.4.0-bin-hadoop3
```

Then type these commands line by line 

```
export SPARK_HOME = 'home/ubuntu/spark-3.4.0-bin-hadoop3'
export PATH =$SPARK_HOME:$PATH
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
```

Then

```
export PYSPARK_DRIVER_PYTHON="jupyter"
export PYSPARK_DRIVER_PYTHON_OPTS="notebook"
export PYSPARK_PYTHON=python3
```

then cd into the spark directory

```
sudo chmod 777 python
sudo chmod 777 pyspark
cd
```
Even cooler way to start PyPark without going in the python and PySpark folder

```
pip3 install findspark
```


## To add pyspark

type ```python3``` and insert the following in the terminal

```
>>> import findspark
>>> findspark.init('/home/camagakhan/spark')
>>> import pyspark
```
In the file ```Links and Terms for Assignment``` you have installations on how to run Zookeeper and Apache Kafka. For convenience sake, I copied the commands here: 

Apache kafka

In the kafka folder:

```
sudo bin/zookeeper-server-start.sh config/zookeeper.properties
```
will run on : 0.0.0.0/0.0.0.0:2181

then: ```sudo bin/kafka-server-start.sh config/server.properties```

---------------------------------------------------------------------------------------------------------------------------

You have a folder called ```Data```, which contains the csv datasets used for this solution. You have AIR_GHG.csv and NASA_SurfaceTemperature.ipynb. These datasets are used. These are now located in the ```Old``` folder. Any datasource I use are located in the ```Data``` folder.

In the ```notebooks``` folder and the ```kafka-config.ipynb``` file you have the basic configurations to create topics and producers for Apache Kafka.

In the ```climatechange.ipynb``` you have everything you need about the climate change polynomial regression. ```StreamProcessing.py``` which is located in the ```scripts``` folder has all the logic related to PySpark. You just need to run the climatechange notebook after first starting kafka and zookeeper, then you executed the kafka-config.ipynb notebook.

In the notebooks directory, the folder is split in to ```scripts``` (these contain the basic structure of the Stream Processing mechanism). You have the ClimateChange.ipynb and the temperature.ipynb notebooks. In the ClimateChange.ipynb we use the scripts to communicate with Apache Kafka and Apache Spark (using PySpark). The clean.py contains logic to delete from the climatechange database. test2.ipynb is just a folder were graphs about emissions are plotted. These are the graphs I was familiarized with when I began this assignment.

# For StreamProcessing.py [Important]

This file is located in ```notebooks > scripts > StreamProcessing.py```. On lines 7 to 9 you need to input the directory you installed your Apache Spark files. Please change the ```username``` variable on line ```7``` with your username, and insert your spark path in the variable ```SPARK_PATH```. I suggest you go to the Spark Folder path in your terminal and type ```pwd``` to print the current working directory, which you will paste on line ```8```. The Path should be similar to this code snippet:

```
username = 'camagakhan' # change to your username.
SPARK_PATH = '/home/{0}/spark-3.4.0-bin-hadoop3'.format(username)
ff.init(SPARK_PATH) # this is the directory I installed spark. If you follow the steps on the readme file, I've highlighted how you can get this directory on Ubuntu. I use this instead of the bashrc command

```

