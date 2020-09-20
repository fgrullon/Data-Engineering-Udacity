import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP IF EXISTS staging_events"
staging_songs_table_drop = "DROP IF EXISTS staging_songs"
songplay_table_drop = "DROP IF EXISTS songplay"
user_table_drop = "DROP IF EXISTS user"
song_table_drop = "DROP IF EXISTS song"
artist_table_drop = "DROP IF EXISTS artist"
time_table_drop = "DROP IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXIST staging_events(
    artist text,
    auth text,
    firstName text not null,
    gender text not null,
    itemInSession integer not null,
    lastName text,
    length numeric,
    level text,
    location text,
    page text,
    sessionId integer not null,
    song text,
    ts numeric not null,
    userAgent text not null,
    userId integer not null
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXIST staging_songs(
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

songplay_table_create = ("""
CREATE TABLE IF NOT EXIST songplay(
    songplay_id integer IDENTITY(0,1),
    start_time numeric not null,
    user_id integer not null,
    level text not null,
    song_id text,
    artist_id text,
    session_id integer not null,
    location text not null,
    user_agent text not null
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXIST user(
    user_id integer,
    first_name text not null,
    last_name text not null,
    gender text not null,
    level text not null
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXIST song(
    song_id text,
    title text  not null,
    artist_id text  not null,
    year integer,
    duration numeric  not null
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXIST artist(
    artist_id text,
    name text  not null,
    location text,
    latitude numeric,
    longitude numeric
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXIST time(
    start_time numeric,
    hour integer NOT NULL,
    day integer NOT NULL,
    week integer NOT NULL,
    month integer NOT NULL,
    year integer NOT NULL,
    weekday integer NOT NULL
)
""")

# STAGING TABLES
staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
""").format(config.LOG_DATA, config.ARN)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
""").format(config.SONG_DATA, config.ARN)

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
