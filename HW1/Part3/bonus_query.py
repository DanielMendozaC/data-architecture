import pandas as pd
import sqlite3

conn = sqlite3.connect('spotify_snowflake.db')

query = """
SELECT 
    a.artist_name,
    SUM(f.play_seconds) / 3600.0 AS total_hours
FROM Plays_Fact f
JOIN Track_Artist_Bridge tab ON f.track_id = tab.track_id
JOIN Dim_Artist a ON tab.artist_id = a.artist_id
GROUP BY a.artist_name
ORDER BY total_hours DESC
LIMIT 10
"""

result = pd.read_sql_query(query, conn)
print(result)

conn.close()