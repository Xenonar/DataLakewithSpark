# Data Lake with Spark

## Overview
The schema for analyses Song play dataset from the start-up called Sparkify
In this project, demonstrate of analytic data using Spark with Amazon EMR 

## Datasets

We will working with two dataset reside in S3 using the following links:

1.Song Play: ```s3a://udacity-dend/song_data/*/*/*/*.json```
2.Log Data: ```s3a://udacity-dend/log_data/*/*/*/*.json```


### Song Play data

In this data, it is a subset of the real data from [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/)
Each file is in JSON format and contains metadata about a song and the artist of that song.

### Log Dataset

This dataset, is consist of log file in JSON format that generated by [Event Simulator](https://github.com/Interana/eventsim) based on the song event from first dataset 

For example
![log_data](https://video.udacity-data.com/topher/2019/February/5c6c3ce5_log-data/log-data.png)

### UDF timestamp convert
In this project we need to convert timestamp into datetime. Due to the native convertion from native pyspark.

### Process data function
In the data process, it has 2 database, song_data and log_data.

Step for Songs, Artists, User tables
- Read data and save into dataframe variable using spark.read
- Extract data into table
- Overwrite data using 'write.mode'

Step for extracting timestamp
- Get data from spark.read
- Covert timestamp using datetime UDF function
- Adding timestamp into table using '.withColumn'

### Files 
1. dl.cfg = Configuration file
2. etl.py = python file for runing ETL process
3. README.md = Instruction and explanation for the project
4. workingspace.ipynb = working space for testing code and checking in Python notebook

### Running
1. Open EMR cluster
2. Add key and secret key to config file
3. Running command '''python3 etl.py'''





