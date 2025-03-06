-- Query to get the maximum daily unique usernames per tenant
SELECT
    Tenant
    MAX(daily_username_count) AS max_daily_username_count
FROM (
    SELECT
        Tenant,
        Timestamp_d,
        COUNT(DISTINCT Username) AS daily_username_count
    FROM tsv8logs
    GROUP BY Tenant, Timestamp_d
) AS daily_counts
GROUP BY Tenant

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
        MAX(daily_username_count) AS max_daily_username_count
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
 AND d.daily_username_count = m.max_daily_username_count;




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

