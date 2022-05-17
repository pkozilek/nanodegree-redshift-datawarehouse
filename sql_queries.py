import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = """
    CREATE TABLE staging_events (
        artist_name VARCHAR,
        auth VARCHAR,
        first_name VARCHAR,
        gender VARCHAR,
        item_in_session INTEGER,
        last_name VARCHAR,
        length NUMERIC,
        level VARCHAR,
        artist_location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration NUMERIC,
        session_id INTEGER,
        song VARCHAR,
        status INTEGER,
        ts NUMERIC,
        user_agent VARCHAR,
        user_id INTEGER
    )
"""

staging_songs_table_create = """
    CREATE TABLE staging_songs (
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_latitude NUMERIC,
        artist_longitude NUMERIC,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration NUMERIC,
        year INTEGER
    )
"""

songplay_table_create = """
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER IDENTITY(1, 1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id INTEGER NOT NULL,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INTEGER,
        artist_location VARCHAR,
        user_agent VARCHAR
    )
"""

user_table_create = """
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR
    )
"""

song_table_create = """
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR,
        artist_id VARCHAR,
        year INTEGER,
        duration NUMERIC
    )
"""

artist_table_create = """
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY,
        artist_name VARCHAR,
        artist_location VARCHAR,
        artist_latitude NUMERIC,
        artist_longitude NUMERIC
    )
"""

time_table_create = """
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER
    )
"""

# STAGING TABLES

staging_events_copy = f"""
    COPY staging_events FROM {LOG_DATA}
    credentials 'aws_iam_role={ARN}'
    format as json {LOG_JSONPATH}
    region 'us-west-2';
"""

staging_songs_copy = f"""
    COPY staging_songs FROM {SONG_DATA}
    credentials 'aws_iam_role={ARN}'
    json 'auto'
    region 'us-west-2';
"""

# FINAL TABLES

songplay_table_insert = """
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        artist_location,
        user_agent
    )
    SELECT
        TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second',
        se.user_id,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.session_id,
        se.artist_location,
        se.user_agent
    FROM staging_events se
    LEFT JOIN staging_songs ss
        ON se.song = ss.title
            AND se.artist_name = ss.artist_name
    WHERE se.page = 'NextSong'
"""

user_table_insert = """
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT
        DISTINCT user_id,
        first_name,
        last_name,
        gender,
        level
    FROM staging_events
    WHERE page = 'NextSong'
"""

song_table_insert = """
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
"""

artist_table_insert = """
    INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
"""

time_table_insert = """
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT
        DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second',
        extract(hour FROM TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second') as hour,
        extract(day FROM TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second') as day,
        extract(week FROM TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second') as week,
        extract(month FROM TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second') as month,
        extract(year FROM TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second') as year,
        extract(weekday FROM TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second') as weekday
    FROM staging_events se
    WHERE page = 'NextSong'
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
