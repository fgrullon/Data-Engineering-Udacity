import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE","ARN")
LOG_DATA = config.get("S3","LOG_DATA")
SONG_DATA = config.get("S3","SONG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplays_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    event_id int identity(0,1),
    artist text,
    auth text,
    firstName text,
    gender text,
    itemInSession integer,
    lastName text,
    length numeric,
    level text,
    location text,
    method text,
    page text,
    registration text,
    sessionId integer not null,
    song text,
    status integer,
    ts numeric not null,
    userAgent text,
    userId integer
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs integer,
    artist_id text not null,
    artist_latitude numeric,
    artist_longitude numeric,
    artist_location text,
    artist_name text not null,
    song_id text not null,
    title text not null,
    duration numeric not null,
    year integer
)
""")


songplays_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id integer IDENTITY(0,1) PRIMARY KEY  ,
    start_time timestamp not null REFERENCES time(start_time) sortkey,
    user_id integer not null REFERENCES users(user_id),
    level text not null,
    song_id text not null REFERENCES songs(song_id),
    artist_id text REFERENCES artists(artist_id) distkey,
    session_id integer not null,
    location text not null,
    user_agent text not null
)
""")


user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id integer primary key sortkey,
    first_name text not null,
    last_name text not null,
    gender text not null,
    level text not null
)diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id text primary key sortkey,
    title text  not null,
    artist_id text not null,
    year integer,
    duration numeric not null
)diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id text primary key sortkey,
    name text  not null,
    location text,
    latitude numeric,
    longitude numeric
)diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp primary key sortkey,
    hour integer NOT NULL,
    day integer NOT NULL,
    week integer NOT NULL,
    month integer NOT NULL,
    year integer NOT NULL,
    weekday integer NOT NULL
)diststyle all;
""")


# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' format as JSON {}
    timeformat as 'epochmillisecs';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' format as JSON 'auto';
""").format(SONG_DATA, ARN)


# FINAL TABLES

songplays_table_insert = ("""
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select distinct  
    timestamp 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
    userId AS user_id, 
    se.level AS level,
    ss.song_id AS song_id, 
    ss.artist_id AS artist_id, 
    se.sessionId AS session_id, 
    se.location AS location, 
    se.userAgent AS user_agent
from staging_events se
join staging_songs ss on (ss.title = se.song and ss.artist_name = se.artist ) 
where se.page = 'NextSong'
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level)
select distinct
    userId AS user_id,
    firstName AS first_name, 
    lastName AS last_name, 
    gender, 
    level
from staging_events
where page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT distinct
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
from staging_songs
""")

artist_table_insert = ("""
insert into artists (artist_id, name, location, latitude, longitude)
SELECT distinct
    artist_id, 
    artist_name AS name, 
    artist_location AS location, 
    artist_latitude AS latitude, 
    artist_longitude AS longitude
from staging_songs
""")

time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
SELECT distinct
    timestamp 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
    extract(hour from start_time) AS hour, 
    extract(day from start_time) AS day, 
    extract(week from start_time) AS week, 
    extract(month from start_time) AS month, 
    extract(year from start_time) AS year, 
    extract(dayofweek from start_time) AS weekday
from staging_events
where page = 'NextSong'
""")

# GET NUMBER OF ROWS IN EACH TABLE
get_number_staging_events = ("""
    SELECT COUNT(*) FROM staging_events
""")

get_number_staging_songs = ("""
    SELECT COUNT(*) FROM staging_songs
""")

get_number_songplays = ("""
    SELECT COUNT(*) FROM songplays
""")

get_number_users = ("""
    SELECT COUNT(*) FROM users
""")

get_number_songs = ("""
    SELECT COUNT(*) FROM songs
""")

get_number_artists = ("""
    SELECT COUNT(*) FROM artists
""")

get_number_time = ("""
    SELECT COUNT(*) FROM time
""")

# QUERY LISTS

create_table_queries = {
    'staging_events' : staging_events_table_create, 
    'staging_songs' : staging_songs_table_create, 
    'user' : user_table_create, 
    'song' : song_table_create, 
    'artist' : artist_table_create, 
    'time' : time_table_create, 
    'songplays' : songplays_table_create
}

drop_table_queries = {
    'staging_events' : staging_events_table_drop, 
    'staging_songs' : staging_songs_table_drop, 
    'songplays' : songplays_table_drop, 
    'user' : user_table_drop, 
    'song' : song_table_drop, 
    'artist' : artist_table_drop, 
    'time' : time_table_drop
}

copy_table_queries = {
    'staging_events' : staging_events_copy, 
    'staging_songs' : staging_songs_copy
}

insert_table_queries = {
    'user' : user_table_insert, 
    'song' : song_table_insert, 
    'artist' : artist_table_insert, 
    'time' : time_table_insert, 
    'songplays' : songplays_table_insert
}
select_number_rows_queries= [
    get_number_staging_events, 
    get_number_staging_songs, 
    get_number_songplays, 
    get_number_users, 
    get_number_songs, 
    get_number_artists, 
    get_number_time
    ]
