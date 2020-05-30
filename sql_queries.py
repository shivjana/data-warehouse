import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs_table (
num_songs integer,
artist_id varchar NOT NULL,
artist_latitude float,
artist_longitude float,
artist_location varchar,
artist_name varchar,
song_id varchar NOT NULL,
title varchar,
duration float,
year int)

""")

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events_table (
artist varchar,
auth varchar,
firstName varchar,
gender char(1),
itemInSession int,
lastName varchar,
length float,
level varchar,
location varchar,
method varchar,
page varchar,
registration float,
sessionId int,
song varchar,
status int,
ts bigint,
userAgent varchar,
userId int)

""")
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id INT IDENTITY(1,1) PRIMARY KEY, start_time timestamp, user_id int, level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, first_name varchar, last_name varchar, gender char(1), level varchar UNIQUE NOT NULL)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY, title varchar, artist_id varchar, year int, duration float)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, name varchar, location varchar, latitude float, longitude float)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY, hour int, day varchar, week int, month varchar, year int, weekday boolean)
""")

# STAGING TABLES

LOG_DATA = config.get('S3','LOG_DATA')
SONG_DATA = config.get('S3','SONG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
DWH_ROLE_ARN = config.get('IAM_ROLE','DWH_ROLE_ARN')
staging_events_copy = ("""COPY staging_events_table FROM {}
CREDENTIALS 'aws_iam_role={}'
region 'us-west-2' FORMAT AS JSON {}
""").format(LOG_DATA,DWH_ROLE_ARN,LOG_JSONPATH)

staging_songs_copy = ("""COPY staging_songs_table FROM {}
CREDENTIALS 'aws_iam_role={}'
region 'us-west-2' FORMAT AS json 'auto'
""").format(SONG_DATA,DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 second' as start_time,
se.userId as user_id,
se.level,
ss.song_id,
ss.artist_id,
se.sessionId as session_id,
se.location,
se.userAgent as user_agent
FROM staging_events_table se 
JOIN staging_songs_table ss ON (se.song = ss.title)
AND (se.artist = ss.artist_name)
WHERE se.page = 'NextSong'
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT userId as user_id,
firstName as first_name,
lastName as last_name,
gender,
level
FROM staging_events_table
WHERE user_id IS NOT NULL
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT song_id,
title,
artist_id,
year,
duration
FROM staging_songs_table
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT artist_id,
artist_name as name,
artist_location as location,
artist_latitude as latitude,
artist_longitude as longitude
FROM staging_songs_table
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 second' as start_time,
EXTRACT(hour FROM start_time) as hour,
EXTRACT(day FROM start_time) as day,
EXTRACT(week FROM start_time) as week,
EXTRACT(month FROM start_time) as month,
EXTRACT(year FROM start_time) as year,
CASE WHEN EXTRACT(dow FROM start_time) NOT IN (5,6) THEN true ELSE false END AS weekday
FROM staging_events_table se
""")

# QUERY LISTS

create_table_queries = [staging_songs_table_create, staging_events_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
