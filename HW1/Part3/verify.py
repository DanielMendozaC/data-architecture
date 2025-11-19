import sqlite3

# Check row counts
conn = sqlite3.connect('spotify_snowflake.db')
cursor = conn.cursor()

tables = ['Plays_Fact', 'Dim_User', 'Dim_Track', 'Dim_Artist', 'Dim_Album', 
          'Dim_Genre', 'Dim_Device', 'Dim_Location', 'Dim_Country', 'Dim_Time']
          
for table in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    print(f"{table}: {cursor.fetchone()[0]} rows")

conn.close()