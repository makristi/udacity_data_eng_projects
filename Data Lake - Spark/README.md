# PURPOSE OF THE PROJECT

## Initial Problem:
Sparkify has grown their user base and song database and is in need to move their data warehouse to a data lake. 

## Solution:
Building an ETL pipeline for a data lake hosted on S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. 

# Data Sources
Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data
Analytics tables parquet files: s3://udacity-dend/analytics
Configurations and connection data: ./dwh.cfg

# SCHEMA

## Fact Table
It is called Songplays and contains records in event data associated with song plays, i.e. records with page 'NextSong'.
    songplay_id|start_time|user_id|level|song_id|artist_id|session_id|location|user_agent

## Dimension Tables
1. users - users in the app
    user_id|first_name|last_name|gender|level
2. songs - songs in music database
    song_id|title|artist_id|year|duration
3. artists - artists in music database
    artist_id|name|location|lattitude|longitude
4. time - timestamps of records in songplays broken down into specific units
    start_time|hour|day|week|month|year|weekday


# EXECUSION OF THE PROJECT
- Run etl to populate tables with information stored in both song and log files
