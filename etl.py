import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS CREDS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data+'song_data/A/A/A/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    df.printSchema()

    # extract columns to create songs table
    songs_table = df.selectExpr(["song_id","title","artist_id","cast(year as int) year","duration"]).orderBy("song_id")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year","artist_id").parquet(output_data+"songs_table/")

    # extract columns to create artists table
    artists_table = df.selectExpr(["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]).orderBy("artist_id")
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data+"artists_table/")
    print('------------------------End Songs and Artists creation ------------------')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data +"log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    df.printSchema()
    
    # filter by actions for song plays
    df = df.filter(df.page=="NextSong")

    # extract columns for users table    
    user_table = df.selectExpr(["userId","firstName","lastName","gender","level"]).orderBy("userId")
    
    # write users table to parquet files
    user_table.write.mode("overwrite").parquet(output_data+"users_table/")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:  datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select('ts','datetime','start_time',year(df.datetime).alias('year'),month(df.datetime).alias('month')).dropDuplicates()
   
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data +"time_table/")

    # read in song data to use for songplays table
    
    print('------- SongPlay Table initiate --------')
    song_data = input_data+'song_data/*/*/*/*.json'
    song_df = spark.read.json(song_data)

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs_table/")
                
    # extract columns from joined song and log datasets to create songplays table
    songplays_table = log_df.join(song_df,(song_df.artist_name == log_df.artist)&(song_df.title == log_df.song)&(log_df.length ==song_df.duration),\
    how='left_outer').withColumn('start_time', get_datetime(log_df.ts))\
    .select('start_time',log_df.userId.alias('user_id'),'level','song_id','artist_id',log_df\
    .sessionId.cast(IntegerType()).alias('session_id'),'location',log_df.userAgent.alias('user_agent'))\
    .withColumn('songplay_id', monotonically_increasing_id())
    
    songplays_table = songplays_table.join(time_table, songplays_table.start_time == time_table.start_time, how="inner")\
    .select("songplay_id",songplays_table.start_time, "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent", "year","month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data+"songplays_table/")
    songplays.printSchema()


def main():
    spark = create_spark_session()
    sc = spark.sparkContext
    #sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", config['AWS CREDS']['AWS_ACCESS_KEY_ID'])
    #sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", config['AWS CREDS']['AWS_SECRET_ACCESS_KEY'])
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()