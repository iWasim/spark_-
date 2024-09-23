sql-
SELECT 
    s.object_id,
    s.object_name,
    s.event_time,
    CASE 
        WHEN s.event_time < NOW() - INTERVAL '1 year' THEN '1y'
        WHEN s.event_time < NOW() - INTERVAL '6 months' THEN '6m'
        ELSE NULL
    END AS unused_dur,
    's3' AS system
FROM 
    s3_table AS s
WHERE 
    s.event_time < NOW() - INTERVAL '1 year'
    OR (s.event_time < NOW() - INTERVAL '6 months' AND s.event_time >= NOW() - INTERVAL '1 year')

UNION ALL

SELECT 
    a.object_id,
    a.object_name,
    a.event_time,
    CASE 
        WHEN a.event_time < NOW() - INTERVAL '1 year' THEN '1y'
        WHEN a.event_time < NOW() - INTERVAL '6 months' THEN '6m'
        ELSE NULL
    END AS unused_dur,
    'abudp' AS system
FROM 
    abudp_table AS a
WHERE 
    a.event_time < NOW() - INTERVAL '1 year'
    OR (a.event_time < NOW() - INTERVAL '6 months' AND a.event_time >= NOW() - INTERVAL '1 year');
