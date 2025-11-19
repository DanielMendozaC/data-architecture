-- Query 1: Top 5 genres by total listening time per country
SELECT 
    l.country,
    t.genre_primary,
    SUM(f.play_seconds) / 3600.0 AS total_hours
FROM Plays_Fact f
JOIN Dim_Track t ON f.track_id = t.track_id
JOIN Dim_Location l ON f.location_id = l.location_id
GROUP BY l.country, t.genre_primary
ORDER BY l.country, total_hours DESC
LIMIT 5;

-- Query 2: Average play duration by subscription tier and platform
SELECT 
    u.subscription_tier,
    d.platform,
    AVG(f.play_seconds) AS avg_play_duration
FROM Plays_Fact f
JOIN Dim_User u ON f.user_id = u.user_id
JOIN Dim_Device d ON f.device_id = d.device_id
GROUP BY u.subscription_tier, d.platform
ORDER BY u.subscription_tier, d.platform;