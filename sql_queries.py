import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplay"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= """create table if not exists staging_events(artist varchar, auth varchar, firstName varchar,                                gender varchar, itemInSession int, lastName varchar, length float, level varchar, location                                  varchar, method varchar, page varchar, registration varchar, sessionId int, song varchar,                                    status int, ts bigint, userAgent varchar, userId int)"""

staging_songs_table_create = """create table if not exists staging_songs (song_id varchar, num_songs int, title varchar,                                  artist_name varchar, artist_latitude varchar, year int, duration float, artist_id varchar,                                  artist_longitude varchar, artist_location varchar)"""

songplay_table_create = """create table if not exists songplay (songplay_id int identity(0,1), start_time varchar, user_id                           int, level varchar, song_id varchar, artist_id varchar, session_id varchar, location varchar,                               user_agent varchar)"""

user_table_create = """create table if not exists users (user_id varchar, first_name varchar, last_name varchar, gender                         varchar, level varchar)"""

song_table_create = """create table if not exists songs (song_id varchar, title varchar, artist_id varchar, year int,                           duration float)"""

artist_table_create = """create table if not exists artists (artist_id varchar, artist_name varchar, artist_location                               varchar, artist_latitude varchar, artist_longitude varchar)"""

time_table_create = """create table if not exists time (start_time varchar, hour varchar, day varchar, week varchar, month                       varchar, year int, weekday varchar)"""

# FINAL TABLES
songplay_table_insert = """insert into songplay (start_time, user_id, level, song_id, artist_id, session_id, location,                               user_agent) select distinct ts, userId, level, song_id, artist_id, sessionId, location, userAgent                             from staging_events join staging_songs on staging_events.song = staging_songs.title and                                       staging_events.artist = staging_songs.artist_name"""

user_table_insert = """insert into users (user_id, first_name, last_name, gender, level) select distinct userId, firstName,                       lastName, gender, level from staging_events"""

song_table_insert = """insert into songs (song_id, title, artist_id, year, duration) select distinct song_id, title,                             artist_id, year, duration from staging_songs"""

artist_table_insert = """insert into artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)                           select distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude from                               staging_songs"""

# Time insert table has been picked up from one of the answers from the 'Mentor Help' section.
time_table_insert = """insert into time (start_time, hour, day, week, month, year, weekday) 
                    select distinct a.start_time, extract (hour from a.start_time), extract (day from a.start_time), extract                     (week from a.start_time), extract (month from a.start_time), extract (year from a.start_time), extract                       (weekday from a.start_time) from (select timestamp 'epoch' + start_time/1000 *interval '1 second' as                         start_time from songplay) a;"""


# STAGING TABLES

staging_events_copy = """copy staging_events from 's3://udacity-dend/log_data' 
    credentials 'aws_iam_role={}' 
    compupdate off region 'us-west-2'
    JSON 's3://udacity-dend/log_json_path.json';
""".format('arn:aws:iam::323643453072:role/dwhRole')

staging_songs_copy = """copy staging_songs from 's3://udacity-dend/song_data' 
    credentials 'aws_iam_role={}' 
    compupdate off region 'us-west-2'
    JSON 'auto' truncatecolumns;
""".format('arn:aws:iam::323643453072:role/dwhRole')

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
