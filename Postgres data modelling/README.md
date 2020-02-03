- Initial Problem:
Currently information about songs and user activities on Sparkify app are stored in json files. This makes reading and analizing the information difficult. 
Analytics team would like to know what songs are the most popular among users.

- Solution:
The purpose of this database is to store information in centralized space and to provide an ability to analyze data. 

- Schema and ETLs:
The database is in star schema with fact table is called songplays and the following dimensions: songs, artists, time and users. The star schema is good when the solution for the specific query/problem is needed. In our case, it is to analyze popularity of songs on the app.
According to business requirements, songs, artists and time will always have exact the same information, even if files contain duplicated records. However, users information is going to be updated with every new instance of the same user_id. 
There was an assumption that both song and log files will always have song_id, user_id, timestamp, but artist_id might be missing sometimes. So, fact table doesn't have FK for artist_id. It also doesn't have PK for song_id, but only because for evaluation the results need to have "none" value. I would actually just not insert records which don't have existing song. 

- To ran the project:
1. Run create_tables script to create database and required tables
2. Run etl to populate tables with information stored in both song and log files
3. The project is ready for running queries

- Examples of queries:
For most often played (even if the most popular song is listened by only one user):
SELECT s.title, a.name, sum(user_id) 
FROM songplays sp
LEFT JOIN songs s ON sp.song_id = s.song_id #would drop left join if song_id in songplays is FK
LEFT JOIN artists a ON a.artist_id = sp.artist_id
GROUP BY s.title, a.name

For most popular song by number of various users:
For most often played (even if the most popular song is listened by only one user):
SELECT s.title, a.name, sum(distinct user_id) 
FROM songplays sp
LEFT JOIN songs s ON sp.song_id = s.song_id #would drop left join if song_id in songplays is FK
LEFT JOIN artists a ON a.artist_id = sp.artist_id
GROUP BY s.title, a.name
