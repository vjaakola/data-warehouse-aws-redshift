import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE", "ARN")

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
CREATE TABLE IF NOT EXISTS staging_events(
    artist          varchar     encode ZSTD, 
    auth            varchar     encode ZSTD,
    firstName       varchar     encode ZSTD,
    gender          varchar     encode ZSTD,
    itemInSession   integer     encode ZSTD,
    lastName        varchar     encode ZSTD,
    length          float       encode ZSTD,
    level           varchar     encode ZSTD,
    location        varchar     encode ZSTD,
    method          varchar     encode ZSTD,
    page            varchar     encode ZSTD,
    registration    bigint      encode ZSTD,
    sessionId       integer     encode ZSTD,
    song            varchar     encode ZSTD,
    status          integer     encode ZSTD,
    ts              bigint      encode ZSTD,
    userAgent       varchar     encode ZSTD,
    user_id         varchar     encode ZSTD
)
""")
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    
    artist_id           varchar     encode ZSTD distkey, 
    artist_latitude     float       encode ZSTD,
    artist_location     varchar     encode ZSTD,
    artist_longitude    float       encode ZSTD,
    artist_name         varchar     encode ZSTD, 
    duration            float       encode ZSTD,
    num_songs           integer     encode ZSTD,
    song_id             varchar     encode ZSTD,
    title               varchar     encode ZSTD,
    year                integer     encode ZSTD
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id     integer encode ZSTD not null    primary key IDENTITY(0,1),
    start_time      timestamp   encode ZSTD     not null    sortkey,
    user_id         varchar     encode ZSTD     not null,
    level           varchar     encode ZSTD,
    song_id         varchar     encode ZSTD,
    artist_id       varchar     encode ZSTD     distkey,
    session_id      integer     encode ZSTD,
    location        varchar     encode ZSTD,
    user_agent      varchar     encode ZSTD
    
); 

""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id         varchar     encode ZSTD not null sortkey     primary key,
    first_name      varchar     encode ZSTD,
    last_name       varchar     encode ZSTD,
    gender          varchar     encode ZSTD,
    level           varchar     encode ZSTD     not null
) diststyle all;

""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id         varchar encode ZSTD not null    sortkey     primary key,
    title           varchar     encode ZSTD,
    artist_id       varchar     encode ZSTD     not null    distkey,
    year            integer     encode ZSTD,
    duration        float       encode ZSTD

); 

""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id       varchar encode ZSTD not null sortkey     primary key,
    name            varchar     encode ZSTD     not null,
    location        varchar     encode ZSTD,
    latitude        float       encode ZSTD,
    longitude       float       encode ZSTD
    
)diststyle auto;

""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time      timestamp  encode ZSTD  not null sortkey     primary key,
    hour            integer     encode ZSTD,
    day             integer     encode ZSTD,
    week            integer     encode ZSTD,
    month           integer     encode ZSTD,
    year            integer     encode ZSTD,
    weekday         integer     encode ZSTD
    
)
diststyle auto;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events from {}
    credentials 'aws_iam_role={}'
    timeformat as 'epochmillisecs'
    JSON {}
    truncatecolumns;
    """).format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)


staging_songs_copy = ("""
COPY staging_songs from {}
    credentials 'aws_iam_role={}'
    JSON 'auto'
    truncatecolumns;
    """).format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' AS start_time,
    e.user_id,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionId as session_id,
    e.location,
    e.userAgent as user_agent
from staging_events e join staging_songs s 
on e.song = s.title and e.artist = s.artist_name
where e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name, 
    gender,
    level
) 
select distinct user_id, firstName, lastName, gender, level
from staging_events
""")


song_table_insert = ("""
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
) 
select distinct song_id, title, artist_id, year, duration 
from staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id, 
    name, 
    location, 
    latitude, 
    longitude
)
select distinct  artist_id, artist_name, artist_location, artist_latitude, artist_longitude
from staging_songs
""")

time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       EXTRACT(WEEKDAY FROM start_time) AS weekday
FROM staging_events;
""")
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
