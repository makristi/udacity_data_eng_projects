import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
artist TEXT, 
auth TEXT, 
first_name TEXT, 
gender TEXT, 
item_in_session INT, 
last_name TEXT, 
length DOUBLE PRECISION, 
level TEXT, 
location TEXT,
method TEXT, 
page TEXT, 
registration BIGINT, 
session_id INT,
song TEXT, 
status INT,
start_time TEXT, 
user_agent TEXT,
user_id INT distkey
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
num_songs INT,
artist_id TEXT,
artist_latitude DOUBLE PRECISION, 
artist_longitude DOUBLE PRECISION, 
artist_location TEXT, 
artist_name TEXT, 
song_id TEXT, 
title TEXT, 
duration DOUBLE PRECISION,  
year INT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
start_time TEXT REFERENCES time(start_time), 
user_id INT REFERENCES users(user_id), 
level TEXT NOT NULL, 
song_id TEXT REFERENCES songs(song_id) sortkey distkey, 
artist_id TEXT REFERENCES artists(artist_id), 
session_id TEXT NOT NULL, 
location TEXT, 
user_agent TEXT
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id INT PRIMARY KEY sortkey, 
first_name TEXT NOT NULL, 
last_name TEXT NOT NULL, 
gender TEXT, 
level TEXT NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id TEXT PRIMARY KEY sortkey, 
title TEXT NOT NULL, 
artist_id TEXT, 
year INT, 
duration DOUBLE PRECISION
); 
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id TEXT PRIMARY KEY sortkey, 
name TEXT NOT NULL, 
location TEXT, 
latitude DOUBLE PRECISION, 
longitude DOUBLE PRECISION
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time TEXT PRIMARY KEY sortkey, 
hour INT NOT NULL, 
day INT NOT NULL, 
week INT NOT NULL, 
month INT NOT NULL, 
year INT NOT NULL, 
weekday INT NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {} 
credentials 'aws_iam_role={}'
region 'us-west-2'
format as json {};
""").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE","ARN"), config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""
copy staging_songs from {} 
credentials 'aws_iam_role={}'
region 'us-west-2'
format as json 'auto';
""").format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT se.start_time, se.user_id, se.level, songs.song_id, artists.artist_id, se.session_id, se.location, se.user_agent
FROM staging_events as se
JOIN time ON time.start_time = se.start_time
JOIN users ON users.user_id = se.user_id
JOIN artists ON lower(artists.name) = lower(se.artist)
JOIN songs ON (songs.title) = (se.song) AND songs.artist_id = artists.artist_id AND songs.duration = se.length
WHERE se.page = 'NextSong'
;""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level) 
SELECT se.user_id, se.first_name, se.last_name, se.gender, se.level
FROM staging_events as se
LEFT JOIN users ON se.user_id = users.user_id
WHERE users.user_id IS NULL
AND se.page = 'NextSong'
;""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration) 
SELECT ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
FROM staging_songs as ss
LEFT JOIN songs ON ss.song_id = songs.song_id
WHERE songs.song_id IS NULL
;""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude) 
SELECT ss.artist_id, ss.artist_name, ss.artist_location, ss.artist_latitude, ss.artist_longitude
FROM staging_songs as ss
LEFT JOIN artists ON ss.artist_id = artists.artist_id
WHERE artists.artist_id IS NULL
;""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday) 
WITH converted_time_set AS(
SELECT start_time, TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second' as converted_time
FROM staging_events
WHERE page = 'NextSong'
)
SELECT cts.start_time, 
extract(hour from converted_time),  
extract(day from converted_time),  
extract(week from converted_time),  
extract(month from converted_time),  
extract(year from converted_time),  
extract(dow from converted_time)
FROM converted_time_set as cts
LEFT JOIN time ON cts.start_time = time.start_time
WHERE  time.start_time IS NULL
;""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
