class SqlQueries:

    # Queries for dropping tables
    staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
    staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
    songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
    user_table_drop = "DROP TABLE IF EXISTS users;"
    artist_table_drop = "DROP TABLE IF EXISTS artists;"
    time_table_drop = "DROP TABLE IF EXISTS time;"
    song_table_drop = "DROP TABLE IF EXISTS songs;"

    # Table Creation Queries
    staging_events_table_create = """
        CREATE TABLE IF NOT EXISTS staging_events (
            artist varchar(256),
	        auth varchar(256),
	        firstname varchar(256),
	        gender varchar(256),
	        iteminsession int,
	        lastname varchar(256),
	        length numeric(18,0),
	        "level" varchar(256),
	        location varchar(256),
	        "method" varchar(256),
	        page varchar(256),
	        registration numeric(18,0),
	        session_id int,
	        song varchar(256),
	        status int,
	        ts int8,
	        useragent varchar(256),
	        userid int
        );
    """

    staging_songs_table_create = """
        CREATE TABLE IF NOT EXISTS staging_songs (
            num_songs int,
	        artist_id varchar(256),
	        artist_name varchar(256),
	        artist_latitude numeric(18,0),
	        artist_longitude numeric(18,0),
	        artist_location varchar(256),
	        song_id varchar(256),
	        title varchar(256),
	        duration numeric(18,0),
	        "year" int
        );
    """

    songplay_table_create = """
        CREATE TABLE IF NOT EXISTS songplays (
            songplay_id VARCHAR PRIMARY KEY,
            start_time TIMESTAMP NOT NULL,
            user_id INT NOT NULL,
            level VARCHAR,
            song_id VARCHAR NOT NULL,
            artist_id VARCHAR NOT NULL,
            session_id INT,
            location VARCHAR,
            user_agent VARCHAR
        );
    """

    user_table_create = """
        CREATE TABLE IF NOT EXISTS users (
            user_id INT PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            gender CHAR(1),
            level VARCHAR NOT NULL
        );
    """

    song_table_create = """
        CREATE TABLE IF NOT EXISTS songs (
            song_id VARCHAR PRIMARY KEY,
            title VARCHAR,
            artist_id VARCHAR NOT NULL,
            year INT,
            duration FLOAT
        );
    """

    artist_table_create = """
        CREATE TABLE IF NOT EXISTS artists (
            artist_id VARCHAR PRIMARY KEY,
            name VARCHAR,
            location VARCHAR,
            latitude FLOAT,
            longitude FLOAT
        );
    """

    time_table_create = """
        CREATE TABLE IF NOT EXISTS time (
            start_time TIMESTAMP PRIMARY KEY,
            hour INT,
            day INT,
            week INT,
            month INT,
            year INT,
            weekday INT
        );
    """

    #Table Insert Queries
    
    songplay_table_insert = """
        SELECT
            md5(events.session_id || events.start_time) songplay_id,
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.session_id,   
            events.location, 
            events.useragent
        FROM
            (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
        LEFT JOIN staging_songs songs
        ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
            WHERE songs.song_id IS NOT NULL;
    """

    user_table_insert = """
        SELECT DISTINCT userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """

    song_table_insert = """
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM staging_songs
    """

    artist_table_insert = """
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """

    time_table_insert = """
        SELECT start_time, EXTRACT(hour FROM start_time), EXTRACT(day FROM start_time), EXTRACT(week FROM start_time),
               EXTRACT(month FROM start_time), EXTRACT(year FROM start_time), EXTRACT(dayofweek FROM start_time)
        FROM songplays
    """