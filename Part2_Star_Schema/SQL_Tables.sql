-- Fact Table
CREATE TABLE Plays_Fact (
    play_id INTEGER PRIMARY KEY,
    user_id VARCHAR(20),
    track_id VARCHAR(20),
    device_id INTEGER,
    location_id INTEGER,
    time_id INTEGER,
    play_seconds INTEGER,
    completed BOOLEAN,
    skips INTEGER
);

-- Dimension Tables
CREATE TABLE Dim_User (
    user_id VARCHAR(20) PRIMARY KEY,
    user_age INTEGER,
    user_country VARCHAR(50),
    subscription_tier VARCHAR(20)
);

CREATE TABLE Dim_Track (
    track_id VARCHAR(20) PRIMARY KEY,
    track_name VARCHAR(200),
    artist_name VARCHAR(200),
    album_name VARCHAR(200),
    album_release_year INTEGER,
    genre_primary VARCHAR(50),
    genre_secondary VARCHAR(50)
);

CREATE TABLE Dim_Device (
    device_id INTEGER PRIMARY KEY,
    device_type VARCHAR(50),
    platform VARCHAR(50),
    os_version VARCHAR(20)
);

CREATE TABLE Dim_Location (
    location_id INTEGER PRIMARY KEY,
    city VARCHAR(100),
    state_province VARCHAR(50),
    country VARCHAR(50)
);

CREATE TABLE Dim_Time (
    time_id INTEGER PRIMARY KEY,
    timestamp TIMESTAMP,
    date DATE,
    hour INTEGER,
    day_of_week INTEGER,
    month INTEGER,
    year INTEGER
);