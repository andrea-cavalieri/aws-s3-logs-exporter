-- Query to get the maximum daily unique usernames per tenant
WITH daily_counts AS (
    SELECT 
        Tenant,
        Timestamp_d,
        COUNT(DISTINCT Username) AS daily_username_count
    FROM tsv8logs
    GROUP BY Tenant, Timestamp_d
),
max_daily AS (
    SELECT 
        Tenant, 
        max(daily_username_count) AS max_daily_username_count
    FROM daily_counts
    GROUP BY Tenant
)
SELECT 
    d.Tenant, 
    d.Timestamp_d, 
    d.daily_username_count AS max_daily_username_count
FROM daily_counts d
JOIN max_daily m
  ON d.Tenant = m.Tenant
 AND d.daily_username_count = m.max_daily_username_count
 ORDER BY max_daily_username_count DESC;


-- UPM - Query to get the average unique usernames per day per tenant
WITH daily_username_counts AS (
    SELECT
         Tenant,
         Timestamp_d,
         COUNT(DISTINCT Username) AS username_count
    FROM tsv8logs
    GROUP BY Tenant, Timestamp_d
)
SELECT 
    Tenant,
    AVG(username_count) AS avg_username_count_per_day
FROM daily_username_counts
GROUP BY Tenant;



-- UPH - Query to get the maximum unique usernames per hour per tenant
WITH daily_counts AS (
    SELECT 
        Tenant,
        Timestamp_h,
        COUNT(DISTINCT Username) AS daily_username_count
    FROM tsv8logs
    GROUP BY Tenant, Timestamp_h
),
ranked AS (
    SELECT 
        Tenant,
        Timestamp_h,
        daily_username_count,
        ROW_NUMBER() OVER (PARTITION BY Tenant ORDER BY daily_username_count DESC, Timestamp_h ASC) AS rn
    FROM daily_counts
)
SELECT Tenant, Timestamp_h, daily_username_count AS max_daily_username_count
FROM ranked
WHERE rn = 1
ORDER BY max_daily_username_count DESC;


-- RPM - Query to get the maximum endpoint count per minute per tenant
SELECT Tenant, Timestamp_m, endpoint_count AS max_endpoint_count
FROM (
    SELECT
         Tenant,
         Timestamp_m,
         COUNT(Endpoint) AS endpoint_count,
         ROW_NUMBER() OVER (PARTITION BY Tenant ORDER BY COUNT(Endpoint) DESC) AS rn
    FROM tsv8logs
    GROUP BY Tenant, Timestamp_m
) sub
WHERE rn = 1 ORDER BY max_endpoint_count DESC;


-- RPS - Query to get the maximum endpoint count per second per tenant
SELECT Tenant, Timestamp_s, endpoint_count AS max_endpoint_count
FROM (
    SELECT
         Tenant,
         Timestamp_s,
         COUNT(Endpoint) AS endpoint_count,
         ROW_NUMBER() OVER (PARTITION BY Tenant ORDER BY COUNT(Endpoint) DESC) AS rn
    FROM tsv8logs
    GROUP BY Tenant, Timestamp_s
) sub
WHERE rn = 1 ORDER BY max_endpoint_count DESC;


-- RPD - Query to get the maximum endpoint count per day per tenant
SELECT Tenant, Timestamp_d, endpoint_count AS max_endpoint_count
FROM (
    SELECT
         Tenant,
         Timestamp_d,
         COUNT(Endpoint) AS endpoint_count,
         ROW_NUMBER() OVER (PARTITION BY Tenant ORDER BY COUNT(Endpoint) DESC) AS rn
    FROM tsv8logs
    GROUP BY Tenant, Timestamp_d
) sub
WHERE rn = 1 ORDER BY max_endpoint_count DESC;

