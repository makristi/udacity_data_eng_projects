import configparser
import os
from datetime import datetime
from pyspark.sql import SparkSession, types as T
from pyspark.sql.functions import udf, col, lower, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear
from pyspark.sql.functions import date_format
from pyspark import SparkContext


config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID') 
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
     """Creates or get existing session for spark"""
    spark = SparkSession.builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
     """
     Reads and transforms data from song_files and stores the final data
     into parquet files for analytics purposes.
     Input parameters are current spark session, filepath for input and output
     files. 
     There is no return, but final result of the function is parquent files for
     song and artist tables.
     """
    # get filepath to song data file
    song_data = input_data + "song_data/A/A/A/*"
    
    # read song data file
    df = spark.read.json(song_data)
    
    # extract columns to create songs table
    songs_table = df.select("song_id","title", "artist_id", "year", "duration") \
        .dropDuplicates(["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id") \
        .parquet(output_data + "/songs", mode="overwrite")

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id as artist_id", \
        "artist_name as name", "artist_location as location", \
        "artist_latitude as latitude", "artist_longitude as longitude") \
        .dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "/artists",mode="overwrite")

def process_log_data(spark, input_data, output_data):
     """
     Reads and transforms data from log_files and stores the final data
     into parquet files for analytics purposes.
     Input parameters are current spark session, filepath for input and output
     files. 
     There is no return, but final result of the function is parquent files for
     time, users and songplays tables.
     """
    # get filepath to log data file
    log_data = input_data + "log_data/2018/11/*"
   
    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df['page']=='NextSong')

    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id", "firstName as first_name", \
        "lastName as last_name", "gender", "level").dropDuplicates(["user_id"])
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "/users",mode="overwrite") 

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), 
                        T.TimestampType()) 
    df = df.withColumn("timestamp", get_timestamp(df.ts))
   
    # extract columns to create time table
    time_table = df.selectExpr("timestamp as start_time", \
        "hour(timestamp) as hour", "day(timestamp) as day", \
        "weekofyear(timestamp) as week","month(timestamp) as month", \
        "year(timestamp) as year", "weekday(timestamp) as weekday") \
        .dropDuplicates(["start_time"])
    
   
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month") \
        .parquet(output_data + "/time",mode="overwrite") 
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "/songs") 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (lower(df.song) == lower(song_df.title))) \
        .selectExpr("monotonically_increasing_id() as songplay_id", \
        "timestamp as start_time", "userId as user_id", "level", "song_id", \
        "artist_id",  "sessionId as session_id", "location", \
        "userAgent as user_agent", "month(timestamp) as month", \
        "year(timestamp) as year")

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.partitionBy("year", "month") \
        .parquet(output_data + "/songplays",mode="overwrite") 



def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/analytics" 
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
