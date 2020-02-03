- Initial Problem:
Currently information about songs and user activities on Sparkify app are stored in json files. This makes reading and analizing the information difficult. 
Analytics team would like to know what songs are the most popular among users.

- Solution:
The purpose of this database is to store information in centralized space and to provide an ability to analyze data. 

PURPOSE OF THE PROJECT
-----------------------------
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They want to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

DATA SOURCES
-----------------------------
Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data
Log data json path: s3://udacity-dend/log_json_path.json
Configurations and connection data: ./dwh.cfg

SCHEMA
-----------------------------
Star schema is used for creating database named dev.

STAGING TABLES
2 staging tables (songs and events) are created for loading raw data from song and log files. After oading, data transformed and loaded to final tables (fact and dimensions).

FACT TABLE
It is called Songplays and contains records in event data associated with song plays, i.e. records with page 'NextSong'.
    songplay_id|start_time|user_id|level|song_id|artist_id|session_id|location|user_agent

DIMENSION TABLES
1. users - users in the app
    user_id|first_name|last_name|gender|level
2. songs - songs in music database
    song_id|title|artist_id|year|duration
3. artists - artists in music database
    artist_id|name|location|lattitude|longitude
4. time - timestamps of records in songplays broken down into specific units
    start_time|hour|day|week|month|year|weekday


EXECUSION OF THE PROJECT
------------------------------
1. Run create_tables script to create database and required tables
2. Run etl to populate tables with information stored in both song and log files
3. The project is ready for running queries

EXAMPLES OF QUERIES
------------------------------
For most often played (even if the most popular song is listened by only one user)
    SELECT s.title, a.name, count(*) 
    FROM songplays sp
    LEFT JOIN songs s ON sp.song_id = s.song_id #would drop left join if song_id in songplays is FK
    LEFT JOIN artists a ON a.artist_id = sp.artist_id
    GROUP BY s.title, a.name

For most popular song by number of various users:
    SELECT s.title, a.name, count(distinct user_id) 
    FROM songplays sp
    LEFT JOIN songs s ON sp.song_id = s.song_id #would drop left join if song_id in songplays is FK
    LEFT JOIN artists a ON a.artist_id = sp.artist_id
    GROUP BY s.title, a.name
