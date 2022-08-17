import configparser
from aws_identity_redshift_cluster_config import AWSCluster


# CONFIG
cluster_info = AWSCluster('dwh.cfg')
log_data = cluster_info.cluster_info['s3_log_data']
log_json_path = cluster_info.cluster_info['s3_log_jsonpath']
song_data = cluster_info.cluster_info['s3_song_data']
region = cluster_info.cluster_info['dwh_region']
host = cluster_info.cluster_info['dwh_host']
ARN = cluster_info.cluster_info['dwh_iam_arn']

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    staging_event_id INT8 IDENTITY(0, 1) PRIMARY KEY,
    artist VARCHAR(300),
    auth VARCHAR(20),
    first_name VARCHAR(300),
    gender VARCHAR(1),
    item_session INT,
    last_name VARCHAR(300),
    length NUMERIC,
    level VARCHAR(10),
    location VARCHAR(300),
    method VARCHAR(20),
    page VARCHAR(300),
    registration NUMERIC,
    session_id INT,
    song VARCHAR(300),
    status INT,
    ts timestamp,
    user_agent VARCHAR(300),
    user_id INT
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    staging_song_id INT8 IDENTITY(0, 1) PRIMARY KEY,
    num_songs INT NOT NULL,
    artist_id VARCHAR(30) NOT NULL,
    artist_latitude NUMERIC,
    artist_longitude NUMERIC,
    artist_location VARCHAR(300),
    artist_name VARCHAR(300) NOT NULL,
    song_id VARCHAR(30) NOT NULL,
    title VARCHAR(300) NOT NULL,
    duration NUMERIC NOT NULL,
    year INT NOT NULL
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
    songplay_id INT8 IDENTITY PRIMARY KEY, 
    start_time TIMESTAMP NOT NULL DISTKEY SORTKEY, 
    user_id INT NOT NULL, 
    level VARCHAR(20), 
    song_id VARCHAR(30), 
    artist_id VARCHAR(30), 
    session_id INT, 
    location VARCHAR(300), 
    user_agent VARCHAR(300)
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id INT NOT NULL PRIMARY KEY, 
    first_name VARCHAR(300), 
    last_name VARCHAR(300), 
    gender VARCHAR(1), 
    level VARCHAR(20)
)
diststyle all
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
    song_id VARCHAR(30) PRIMARY KEY, 
    title VARCHAR(300) NOT NULL, 
    artist_id VARCHAR(30) NOT NULL, 
    year INT NOT NULL, 
    duration NUMERIC NOT NULL
)
diststyle all
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id VARCHAR(30) PRIMARY KEY NOT NULL, 
    name VARCHAR(300) NOT NULL, 
    location VARCHAR(300), 
    latitude NUMERIC, 
    longitude NUMERIC
)
diststyle all
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time TIMESTAMP PRIMARY KEY, 
    hour INT2 NOT NULL, 
    day INT2 NOT NULL, 
    week INT2 NOT NULL, 
    month INT2 NOT NULL, 
    year INT2 NOT NULL, 
    weekday INT2 NOT NULL
)
diststyle all
""")

# STAGING TABLES
staging_events_copy = ("""
COPY staging_events 
FROM {}
CREDENTIALS 'aws_iam_role={}'
JSON {} REGION '{}'
TIMEFORMAT as 'epochmillisecs';
""").format(log_data, ARN, log_json_path, 'us-west-2')

staging_songs_copy = ("""
COPY staging_songs 
FROM {}
CREDENTIALS 'aws_iam_role={}'
JSON 'auto' REGION '{}';
""").format(song_data, ARN, 'us-west-2')

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays (
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent
)
SELECT 
    se.ts,
    se.user_id,
    se.level,
    sa.song_id,
    sa.artist_id,
    se.session_id,
    se.location,
    se.user_agent
FROM staging_events AS se
JOIN 
(
    SELECT 
        s.song_id, a.artist_id, s.title AS song_name, 
        a.name AS artist_name, s.duration AS duration
    FROM songs AS s
    JOIN artists AS a ON s.artist_id = a.artist_id
) AS sa
ON se.song=sa.song_name AND se.artist=sa.artist_name AND se.length=sa.duration;
""")

user_table_insert = ("""
INSERT INTO users 
(
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level
)
SELECT 
    DISTINCT(user_id),
    first_name,
    last_name,
    gender,
    level
FROM staging_events
WHERE user_id IS NOT NULL AND page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs 
(
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
)
SELECT
    DISTINCT(song_id),
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists
(
    artist_id, 
    name, 
    location, 
    latitude, 
    longitude
)
SELECT
    DISTINCT(artist_id),
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time
(
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday 
)
SELECT
    DISTINCT(ts), 
    EXTRACT(HOUR FROM ts) AS hour,
    EXTRACT(DAY FROM ts) AS day,
    EXTRACT(WEEK FROM ts) AS week,
    EXTRACT(MONTH FROM ts) AS month,
    EXTRACT(YEAR FROM ts) AS year,
    EXTRACT(DOW FROM ts) AS weekday
FROM staging_events
WHERE ts IS NOT NULL;
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        songplay_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create]
drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]
copy_table_queries = [staging_events_copy,
                      staging_songs_copy]
insert_table_queries = [user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert,
                        songplay_table_insert]
