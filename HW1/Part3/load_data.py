import sqlite3
import pandas as pd

# Load original data
df = pd.read_csv('../data/spotify_plays.csv')
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Connect to new database
conn = sqlite3.connect('spotify_snowflake.db')

# Create Artist dimension
artists = df[['artist_name']].drop_duplicates().reset_index(drop=True)
artists['artist_id'] = range(1, len(artists) + 1)
dim_artist = artists[['artist_id', 'artist_name']]

# Create Album dimension
albums = df[['album_name', 'album_release_year']].drop_duplicates().reset_index(drop=True)
albums['album_id'] = range(1, len(albums) + 1)
dim_album = albums[['album_id', 'album_name', 'album_release_year']]

# Create Track dimension (with artist_id and album_id)
tracks = df[['track_id', 'track_name', 'artist_name', 'album_name']].drop_duplicates()
tracks = tracks.merge(artists, on='artist_name')
tracks = tracks.merge(albums, on='album_name')
dim_track = tracks[['track_id', 'track_name', 'artist_id', 'album_id']].drop_duplicates(subset=['track_id'])  

# Create Genre dimension
dim_genre = df[['track_id', 'genre_primary', 'genre_secondary']].drop_duplicates()

# Create Country dimension
countries = df[['country']].drop_duplicates().reset_index(drop=True)
countries['country_id'] = range(1, len(countries) + 1)
dim_country = countries[['country_id', 'country']]

# Create Location dimension (with country_id)
locations = df[['city', 'state_province', 'country']].drop_duplicates().reset_index(drop=True)
locations = locations.merge(countries, on='country')
locations['location_id'] = range(1, len(locations) + 1)
dim_location = locations[['location_id', 'city', 'state_province', 'country_id']]

# Create other dimensions (same as star schema)
dim_user = df[['user_id', 'user_age', 'user_country', 'subscription_tier']].drop_duplicates()

device_combos = df[['device_type', 'platform', 'os_version']].drop_duplicates()
device_combos['device_id'] = range(1, len(device_combos) + 1)
dim_device = device_combos[['device_id', 'device_type', 'platform', 'os_version']]

time_data = pd.DataFrame({
    'time_id': range(1, len(df) + 1),
    'timestamp': df['timestamp'],
    'date': df['timestamp'].dt.date,
    'hour': df['timestamp'].dt.hour,
    'day_of_week': df['timestamp'].dt.dayofweek,
    'month': df['timestamp'].dt.month,
    'year': df['timestamp'].dt.year
})

# Create fact table
fact = df.merge(device_combos, on=['device_type', 'platform', 'os_version'])
fact = fact.merge(locations[['city', 'state_province', 'country', 'location_id']], 
                  on=['city', 'state_province', 'country'])
fact['time_id'] = range(1, len(fact) + 1)
fact_table = fact[['play_id', 'user_id', 'track_id', 'device_id', 'location_id', 
                   'time_id', 'play_seconds', 'completed', 'skips']]

# Load all tables
dim_artist.to_sql('Dim_Artist', conn, if_exists='replace', index=False)
dim_album.to_sql('Dim_Album', conn, if_exists='replace', index=False)
dim_track.to_sql('Dim_Track', conn, if_exists='replace', index=False)
dim_genre.to_sql('Dim_Genre', conn, if_exists='replace', index=False)
dim_country.to_sql('Dim_Country', conn, if_exists='replace', index=False)
dim_location.to_sql('Dim_Location', conn, if_exists='replace', index=False)
dim_user.to_sql('Dim_User', conn, if_exists='replace', index=False)
dim_device.to_sql('Dim_Device', conn, if_exists='replace', index=False)
time_data.to_sql('Dim_Time', conn, if_exists='replace', index=False)
fact_table.to_sql('Plays_Fact', conn, if_exists='replace', index=False)

conn.close()
print("Snowflake schema loaded!")