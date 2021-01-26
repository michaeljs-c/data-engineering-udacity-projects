import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
LOG_JSON_PATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events 
(
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender VARCHAR,
itemInSession INTEGER,
lastName VARCHAR,
length FLOAT,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration BIGINT,
sessionId INTEGER,
song VARCHAR,
status INTEGER,
ts BIGINT,
userAgent VARCHAR,
userId INTEGER
)
""")


staging_songs_table_create = ("""
CREATE TABLE staging_songs 
(
num_songs INT,
artist_id VARCHAR,
artist_latitude VARCHAR,
artist_longitude VARCHAR,
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR,
title VARCHAR,
duration FLOAT,
year INT
)
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
    start_time timestamp NOT NULL sortkey distkey, 
    user_id INT NOT NULL, 
    level VARCHAR NOT NULL, 
    song_id VARCHAR, 
    artist_id VARCHAR, 
    session_id INT NOT NULL, 
    location VARCHAR NOT NULL, 
    user_agent VARCHAR NOT NULL
)
""")

user_table_create = ("""
CREATE TABLE users (
    user_id INT PRIMARY KEY sortkey, 
    first_name VARCHAR NOT NULL, 
    last_name VARCHAR NOT NULL, 
    gender VARCHAR NOT NULL, 
    level VARCHAR NOT NULL
)
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id VARCHAR PRIMARY KEY sortkey,
    title VARCHAR NOT NULL, 
    artist_id VARCHAR NOT NULL, 
    year INT NOT NULL, 
    duration FLOAT NOT NULL
)
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id VARCHAR PRIMARY KEY sortkey, 
    name VARCHAR NOT NULL,
    location VARCHAR, 
    latitude VARCHAR, 
    longitude VARCHAR
)
""")

time_table_create = ("""
CREATE TABLE time (
    start_time TIMESTAMP PRIMARY KEY sortkey distkey, 
    hour INT NOT NULL, 
    day INT NOT NULL, 
    week INT NOT NULL, 
    month INT NOT NULL, 
    year INT NOT NULL, 
    weekday INT NOT NULL
)
""")

# STAGING TABLES
staging_events_copy = ("""
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
format as json {}
REGION 'us-west-2'
""").format(LOG_DATA, IAM_ROLE, LOG_JSON_PATH)

staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
format as json 'auto'
REGION 'us-west-2'
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time,
userId, level, song_id, artist_id, sessionId, location, userAgent
FROM staging_events e
JOIN staging_songs s ON e.song = s.title AND e.artist = s.artist_name AND e.length = s.duration
""")

user_table_insert = ("""
INSERT INTO users
SELECT userId, firstName, lastName, gender, level
FROM staging_events
WHERE userId IS NOT NULL""")

song_table_insert = ("""
INSERT INTO songs
SELECT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL""")

time_table_insert = ("""
INSERT INTO time
SELECT start_time, 
        EXTRACT(hour from start_time),
        EXTRACT(day from start_time),
        EXTRACT(week from start_time),
        EXTRACT(month from start_time),
        EXTRACT(year from start_time),
        EXTRACT(dayofweek from start_time)
FROM songplays
""")

analytics_query = """
SELECT count(*) FROM"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
