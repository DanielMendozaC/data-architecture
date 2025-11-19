import sqlite3
import pandas as pd

conn = sqlite3.connect('spotify_snowflake.db')
cursor = conn.cursor()

# 1. Create bridge table
# cursor.execute("""
# CREATE TABLE Track_Artist_Bridge (
#     track_id VARCHAR(20),
#     artist_id INTEGER,
#     PRIMARY KEY (track_id, artist_id)
# )
# """)

# 2. Load bridge data
df = pd.read_csv('../data/spotify_plays.csv')

# Get artist IDs
dim_artist = pd.read_sql_query("SELECT * FROM Dim_Artist", conn)

# Create bridge
bridge = df[['track_id', 'artist_name']].drop_duplicates()
bridge = bridge.merge(dim_artist, on='artist_name')
bridge = bridge[['track_id', 'artist_id']]

bridge.to_sql('Track_Artist_Bridge', conn, if_exists='replace', index=False)

# 3. Update Dim_Track - remove artist_id column
cursor.execute("CREATE TABLE Dim_Track_New AS SELECT track_id, track_name, album_id FROM Dim_Track")
cursor.execute("DROP TABLE Dim_Track")
cursor.execute("ALTER TABLE Dim_Track_New RENAME TO Dim_Track")

conn.commit()
conn.close()