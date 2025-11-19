import sqlite3
import pandas as pd

# Connect to database
conn = sqlite3.connect('spotify_star.db')

# Load CSV
df = pd.read_csv('../data/spotify_plays.csv')

# Create dimension dataframes
dim_user = df[['user_id', 'user_age', 'user_country', 'subscription_tier']].drop_duplicates()
dim_track = df[['track_id', 'track_name', 'artist_name', 'album_name', 
                'album_release_year', 'genre_primary', 'genre_secondary']].drop_duplicates()

# Create device dimension with IDs
device_combos = df[['device_type', 'platform', 'os_version']].drop_duplicates()
device_combos['device_id'] = range(1, len(device_combos) + 1)
dim_device = device_combos

# Create location dimension with IDs
location_combos = df[['city', 'state_province', 'country']].drop_duplicates()
location_combos['location_id'] = range(1, len(location_combos) + 1)
dim_location = location_combos

# Create time dimension
df['timestamp'] = pd.to_datetime(df['timestamp'])
time_data = pd.DataFrame({
    'time_id': range(1, len(df) + 1),
    'timestamp': df['timestamp'],
    'date': df['timestamp'].dt.date,
    'hour': df['timestamp'].dt.hour,
    'day_of_week': df['timestamp'].dt.dayofweek,
    'month': df['timestamp'].dt.month,
    'year': df['timestamp'].dt.year
})

# Merge to create fact table
fact = df.merge(device_combos, on=['device_type', 'platform', 'os_version'])
fact = fact.merge(location_combos, on=['city', 'state_province', 'country'])
fact['time_id'] = range(1, len(fact) + 1)

fact_table = fact[['play_id', 'user_id', 'track_id', 'device_id', 'location_id', 
                   'time_id', 'play_seconds', 'completed', 'skips']]

# Load to database
dim_user.to_sql('Dim_User', conn, if_exists='replace', index=False)
dim_track.to_sql('Dim_Track', conn, if_exists='replace', index=False)
dim_device.to_sql('Dim_Device', conn, if_exists='replace', index=False)
dim_location.to_sql('Dim_Location', conn, if_exists='replace', index=False)
time_data.to_sql('Dim_Time', conn, if_exists='replace', index=False)
fact_table.to_sql('Plays_Fact', conn, if_exists='replace', index=False)

conn.close()