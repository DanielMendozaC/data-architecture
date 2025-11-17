import pandas as pd
import sqlite3

conn = sqlite3.connect('spotify_star.db')

# Query 1
query1 = """
SELECT 
    l.country,
    t.genre_primary,
    SUM(f.play_seconds) / 3600.0 AS total_hours
FROM Plays_Fact f
JOIN Dim_Track t ON f.track_id = t.track_id
JOIN Dim_Location l ON f.location_id = l.location_id
GROUP BY l.country, t.genre_primary
ORDER BY l.country, total_hours DESC
"""
result1 = pd.read_sql_query(query1, conn)
print("Query 1 Results:")
print(result1)

# Query 2
query2 = """
SELECT 
    u.subscription_tier,
    d.platform,
    AVG(f.play_seconds) AS avg_play_duration
FROM Plays_Fact f
JOIN Dim_User u ON f.user_id = u.user_id
JOIN Dim_Device d ON f.device_id = d.device_id
GROUP BY u.subscription_tier, d.platform
ORDER BY u.subscription_tier, d.platform
"""
result2 = pd.read_sql_query(query2, conn)
print("\nQuery 2 Results:")
print(result2)

conn.close()

