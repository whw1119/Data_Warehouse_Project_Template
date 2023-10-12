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

staging_events_table_create = ("""
CREATE TABLE "staging_events"(
  artist             TEXT,
  auth               TEXT,
  firstName          TEXT,
  gender             TEXT,
  itemInSession      INT,
  lastName           TEXT,
  length             DOUBLE PRECISION,
  level              TEXT,
  location           TEXT,
  method             TEXT,
  page               TEXT,
  registration       DOUBLE PRECISION,
  sessionId          INT,
  song               TEXT,
  status             INT,
  ts                 TEXT,
  userAgent          TEXT,
  userId             INT
)
""")

staging_songs_table_create = ("""
CREATE TABLE "staging_songs" (
  num_songs          INT,
  artist_id          TEXT,
  artist_latitude    TEXT,
  artist_longitude   TEXT,
  artist_location    varchar(max),
  artist_name        varchar(max),
  song_id            TEXT,
  title              varchar(max),
  duration           DOUBLE PRECISION,
  year               INT
)
""")

songplay_table_create = ("""
CREATE TABLE "songplays" (
  songplay_id   int IDENTITY(0,1) PRIMARY KEY,
  start_time    TIMESTAMP,
  user_id       INT,
  level         TEXT,
  song_id       INT,
  artist_id     INT,
  session_id    INT,
  location      TEXT,
  user_agent    TEXT
)
""")

user_table_create = ("""
CREATE TABLE "users" (
  user_id       TEXT PRIMARY KEY,
  first_name    TEXT,
  last_name     TEXT,
  gender        char(1),
  level         TEXT
)
""")

song_table_create = ("""
CREATE TABLE "songs" (
  song_id       TEXT PRIMARY KEY,
  title         TEXT,
  artist_id     INT,
  year          INT,
  duration      DOUBLE PRECISION
)
""")

artist_table_create = ("""
CREATE TABLE "artists" (
  artist_id     TEXT PRIMARY KEY,
  name          TEXT,
  location      TEXT,
  lattitude     TEXT,
  longitude     TEXT
)
""")

time_table_create = ("""
CREATE TABLE "time" (
  start_time    TIMESTAMP PRIMARY KEY,
  hour          INT,
  day           INT,
  week          INT,
  month         INT,
  year          INT,
  weekday       INT
)
""")

# STAGING TABLES

DWH_ROLE_ARN = config['IAM_ROLE']['ARN']
table_log_data = config['S3']['LOG_DATA']
JSONPATH = config['S3']['LOG_JSONPATH']
table_song_data = config['S3']['SONG_DATA']

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json {};
""").format(table_log_data, DWH_ROLE_ARN, JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json 'auto'
    TRUNCATECOLUMNS;
""").format(table_song_data, DWH_ROLE_ARN)
  # ACCEPTINVCHARS
  # TRUNCATECOLUMNS

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' as start_time,
        e.userId        as user_id,
        e.level         as level,
        s.song_id       as song_id,
        s.artist_id     as artist_id,
        e.sessionId     as session_id,
        e.location      as location,
        e.userAgent     as user_agent
    from staging_events e
    join staging_songs  s
    on e.song = s.title and e.artist = s.artist_name and e.page = 'NextSong' and e.length = s.duration
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level)
    select
        distinct(userId)    as user_id,
        firstName           as first_name,
        lastName            as last_name,
        gender,
        level
    from staging_events
    where user_id is not null
    and page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs SELECT DISTINCT (song_id)
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists SELECT DISTINCT (artist_id)
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time
        WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts FROM staging_events)
        SELECT DISTINCT
        ts,
        extract(hour from ts),
        extract(day from ts),
        extract(week from ts),
        extract(month from ts),
        extract(year from ts),
        extract(weekday from ts)
        FROM temp_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
