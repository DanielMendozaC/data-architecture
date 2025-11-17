import sqlite3

conn = sqlite3.connect('spotify_star.db')

# 1. Check tables exist
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
print("Tables:", cursor.fetchall())

# 2. Check row counts
tables = ['Plays_Fact', 'Dim_User', 'Dim_Track', 'Dim_Device', 'Dim_Location', 'Dim_Time']
for table in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    print(f"{table}: {cursor.fetchone()[0]} rows")

# 3. Preview data
cursor.execute("SELECT * FROM Plays_Fact LIMIT 5")
print("\nSample Fact data:", cursor.fetchall())

conn.close()