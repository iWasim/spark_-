CREATE OR REPLACE TABLE your_new_table_name AS
SELECT 
    object_id, 
    view_name, 
    CASE 
        WHEN MAX(event_time) < DATE_SUB(current_date(), INTERVAL 1 YEAR) THEN '1y'
        WHEN MAX(event_time) < DATE_SUB(current_date(), INTERVAL 6 MONTH) THEN '6m'
    END AS flag
FROM 
    your_table_name
GROUP BY 
    object_id, 
    view_name
HAVING 
    MAX(event_time) < DATE_SUB(current_date(), INTERVAL 6 MONTH)
    OR MAX(event_time) < DATE_SUB(current_date(), INTERVAL 1 YEAR)

=========================================================================

CREATE OR REPLACE TABLE your_new_table_name AS
SELECT 
    object_id, 
    view_name, 
    CASE 
        WHEN MAX(event_time) < DATE_SUB(current_date(), 365) THEN '1y'
        WHEN MAX(event_time) < DATE_SUB(current_date(), 180) THEN '6m'
    END AS flag
FROM 
    your_table_name
GROUP BY 
    object_id, 
    view_name
HAVING 
    MAX(event_time) < DATE_SUB(current_date(), 180)
    OR MAX(event_time) < DATE_SUB(current_date(), 365)
