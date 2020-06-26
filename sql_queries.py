import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

#staging events table follows format of JSON file, some fields will not be used in final load.
staging_events_table_create= ("""
CREATE TABLE staging_events(
    artist_id varchar,
    auth varchar,
    first_name varchar,
    gender varchar,   
    item_in_session varchar,
    last_name varchar,   
    length float,
    level varchar,    
    location varchar,    
    method varchar,
    page varchar,
    registration varchar,
    session_id int,       
    song_title varchar, 
    status varchar,
    ts bigint,
    user_agent varchar,    
    user_id int
    );
""")

#staging songs table follows format of JSON file, some fields will not be used in final load.
staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    num_songs int,
    artist_id varchar,    
    latitude float,
    longitude float,
    location varchar,      
    name varchar,    
    song_id varchar,
    title varchar,
    duration float,
    year int 
    );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY, 
    start_time timestamp NOT NULL, 
    user_id int, 
    level varchar, 
    song_id varchar, 
    artist_id varchar,
    session_id int, 
    location varchar, 
    user_agent varchar
    );
    """)

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY, 
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar
    );
    """)

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY, 
    song_title varchar, 
    artist_id varchar NOT NULL, 
    year int, 
    duration double precision
    );
    """)

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY, 
    name varchar, 
    location varchar, 
    latitude double precision, 
    longitude double precision
    );
    """)

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int
    );
    """)

# STAGING TABLES

staging_events_copy = ("""
copy staging_events 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2' 
    JSON {}
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
copy staging_songs
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2' 
    FORMAT AS JSON 'auto'
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))


# FINAL TABLES

#changed join to left join
songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
    TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time,  
    e.user_id,
    e.level,
    s.song_id,
    e.artist_id,
    e.session_id,
    e.location,
    e.user_agent    
FROM staging_events e
LEFT JOIN staging_songs s
ON e.song_title = s.title
AND e.artist_id = s.artist_id
WHERE page = 'NextSong';
""")

#same user will appear multiple times, so added "distinct" to prevent duplication
user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    e.user_id, 
    e.first_name, 
    e.last_name, 
    e.gender, 
    e.level
FROM staging_events e
WHERE user_id NOT IN (SELECT DISTINCT user_id FROM users) AND user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, song_title, artist_id, year, duration)
SELECT DISTINCT
    s.song_id, 
    s.title AS song_title, 
    s.artist_id, 
    s.year, 
    s.duration
FROM staging_songs s
WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs) AND song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    s.artist_id, 
    s.name, 
    s.location, 
    s.latitude, 
    s.longitude
FROM staging_songs s
WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists) AND artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT
    TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time,  
    EXTRACT (hour from start_time) AS hour,
    EXTRACT (day from start_time) AS day,
    EXTRACT (week from start_time) AS week,
    EXTRACT (month from start_time) AS month,
    EXTRACT (year from start_time) AS year,
    EXTRACT (dow from start_time) AS weekday
FROM staging_events e;
""")

# QUERY LISTS
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
