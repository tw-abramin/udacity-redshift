import aws_helper

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
test_table = (
    """
    CREATE TABLE IF NOT EXISTS test (
        test_id INT,
        tes_name CHAR)
    """
)
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
artist_name VARCHAR,
auth VARCHAR,
first_name VARCHAR,
last_name VARCHAR,
gender CHAR,
item_in_session INTEGER,
length FLOAT,
level CHAR,
location VARCHAR,
method CHAR,
page CHAR,
registration FLOAT,
session_id INTEGER,
song_title VARCHAR,
status SMALLINT,
start_time BIGINT,
user_agent VARCHAR,
user_id VARCHAR)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
artist_id VARCHAR,
artist_latitude FLOAT,
artist_location FLOAT,
artist_longitude FLOAT,
artist_name VARCHAR,
duration FLOAT,
num_songs INTEGER,
song_id VARCHAR,
title VARCHAR,
year INTEGER)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id VARCHAR NOT NULL PRIMARY KEY SORTKEY,
first_name VARCHAR,
last_name VARCHAR,
gender CHAR,
level VARCHAR)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id VARCHAR NOT NULL PRIMARY KEY SORTKEY,
title VARCHAR,
artist_id VARCHAR REFERENCES artists (artist_id),
year INTEGER,
duration FLOAT)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id VARCHAR NOT NULL PRIMARY KEY SORTKEY,
name VARCHAR,
location VARCHAR,
latitude FLOAT,
longitude FLOAT)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id INT IDENTITY(0,1) NOT NULL PRIMARY KEY SORTKEY,
start_time BIGINT,
user_id VARCHAR REFERENCES users (user_id),
level VARCHAR,
song_id VARCHAR REFERENCES songs (song_id),
artist_id VARCHAR REFERENCES artists (artist_id),
session_id VARCHAR,
location VARCHAR,
user_agent VARCHAR)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time TIMESTAMP NOT NULL PRIMARY KEY SORTKEY,
hour INTEGER,
day INTEGER,
week INTEGER,
month INTEGER,
year INTEGER,
weekday INTEGER)
""")

# STAGING TABLES
## TODO: remove hardcoded ARN
staging_events_copy = ("""
copy staging_events from 's3://udacity-dend/song_data' 
credentials 'aws_iam_role=arn:aws:iam::590606803980:role/DWH_IAM_REDSHIFT_TEST_ROLE'
format as json 'auto' region 'us-west-2';
""").format()

staging_songs_copy = ("""
copy staging_songs from 's3://udacity-dend/log_data' 
credentials 'aws_iam_role=arn:aws:iam::590606803980:role/DWH_IAM_REDSHIFT_TEST_ROLE'
format as json 'auto' region 'us-west-2';
""").format()

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT se.start_time as start_time,
                se.user_id as user_id,
                se.level as level,
                se.song_id as song_id,
                ss.artist_id as artist_id,
                se.session_id as session_id,
                se.location as location,
                se.user_agent as user_agent
FROM staging_events se 
JOIN staging_songs ss ON se.song_id = ss.song_id;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT se.user_id as user_id,
                se.first_name as first_name,
                se.last_name as last_name,
                se.gender as gender,
                se.level as level
FROM staging_events se;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT ss.song_id as song_id,
                se.song_title as title,
                ss.artist_id as artist_id,
                ss.year as year,
                ss.duration as duration
FROM staging_events se 
JOIN staging_songs ss ON se.song_title = ss.title;
""")

artist_table_insert = ("""
INSERT INTO songs (artist_id, name, location, latitude, longitude)
SELECT DISTINCT ss.artist_id as artist_id,
                se.artist_name as name,
                se.location as location,
                ss.artist_latitude as latitude,
                ss.artist_longitude as longitude
FROM staging_events se 
JOIN staging_songs ss ON se.artist_name = ss.artist_name;
""")

time_table_insert = ("""INSERT INTO TIME (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT se.start_time,
                EXTRACT (HOUR FROM se.start_time), 
                EXTRACT (DAY FROM se.start_time),
                EXTRACT (WEEK FROM se.start_time), 
                EXTRACT (MONTH FROM se.start_time),
                EXTRACT (YEAR FROM se.start_time), 
                EXTRACT (WEEKDAY FROM se.start_time) 
FROM (SELECT TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second' as start_time FROM staging_events) se;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, songplay_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
