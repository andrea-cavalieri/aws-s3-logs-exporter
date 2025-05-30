-- UPD - Query to get the maximum unique usernames per day per tenant
WITH daily_counts AS (
    SELECT 
        Tenant,
        Timestamp_d,
        COUNT(DISTINCT Username) AS daily_username_count
    FROM tsv8logs
    GROUP BY Tenant, Timestamp_d
),
ranked AS (
    SELECT 
        Tenant,
        Timestamp_d,
        daily_username_count,
        ROW_NUMBER() OVER (PARTITION BY Tenant ORDER BY daily_username_count DESC, Timestamp_d ASC) AS rn
    FROM daily_counts
)
SELECT Tenant, Timestamp_d, daily_username_count AS max_daily_username_count
FROM ranked
WHERE rn = 1
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
    CAST(AVG(username_count) AS INTEGER) AS avg_username_count_per_day
FROM daily_username_counts
GROUP BY Tenant;




-- UPH - Query to get the maximum unique usernames per hour per tenant
WITH hourly_counts AS (
    SELECT 
        Tenant,
        Timestamp_h,
        COUNT(DISTINCT Username) AS hour_username_count
    FROM tsv8logs
    GROUP BY Tenant, Timestamp_h
),
ranked AS (
    SELECT 
        Tenant,
        Timestamp_h,
        hour_username_count,
        ROW_NUMBER() OVER (PARTITION BY Tenant ORDER BY hour_username_count DESC, Timestamp_h ASC) AS rn
    FROM hourly_counts
)
SELECT Tenant, Timestamp_h, hour_username_count AS max_hour_username_count
FROM ranked
WHERE rn = 1
ORDER BY max_hour_username_count DESC;


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


-- duration - Query to get the average duration per endpoint per tenant
select Tenant, Endpoint, CAST(avg(Duration)/1000 AS INTEGER) as duration from tsv8logs group by Tenant,Endpoint order by  duration desc LIMIT 40

-- duration - Query to get the maximum duration per endpoint per tenant 
select Tenant, Endpoint, CAST(max(Duration)/1000 AS INTEGER) as duration from tsv8logs group by Tenant,Endpoint order by  duration desc LIMIT 30

-- duration - Query to get the average duration per endpoint per tenant - TSV9 - heraeus
 select uri, avg(duration) from  s3('https://tsv9-monitoring-bucket.s3.eu-north-1.amazonaws.com/tenant=heraeus/**')
 group by uri order by avg(duration) desc 

-- TSV9

select tenant, uri, CAST(avg(duration)/1000 AS INTEGER) as duration 
from s3('https://tsv9-monitoring-bucket.s3.eu-north-1.amazonaws.com/tenant=*/**') 
group by tenant,uri order by  duration desc LIMIT 40

-- duration - Query to get the maximum duration per endpoint per tenant - TSV9 - heraeus
select uri, cast(avg(duration)/1000 as integer) as duration_s from  s3('https://tsv9-monitoring-bucket.s3.eu-north-1.amazonaws.com/tenant=heraeus/**')
 group by uri order by avg(duration) desc limit 20


WITH daily_duration_counts AS (
    SELECT
         tenant,
         toDate(timestamp) AS day,
         AVG(duration) AS avg_duration
    FROM s3('https://tsv9-monitoring-bucket.s3.eu-north-1.amazonaws.com/tenant=*/**')
    GROUP BY tenant, day
)
SELECT
    tenant,
    CAST(AVG(avg_duration) AS Int32) AS avg_duration_per_day
FROM daily_duration_counts

-- username - Query to get the average unique usernames per day per tenant - TSV9
  SELECT
         tenant,
         toDate(timestamp) AS day,
         COUNT(DISTINCT username) AS username_count
    FROM s3('https://tsv9-monitoring-bucket.s3.eu-north-1.amazonaws.com/tenant=*/**')
    GROUP BY tenant, day
)
SELECT
    tenant,
    CAST(AVG(username_count) AS Int32) AS avg_username_count_per_day
FROM daily_username_counts
GROUP BY tenant;


-- username - Query to get the maximum unique usernames per hour per tenant - TSV9
WITH hourly_username_counts AS (
    SELECT
         tenant,
         toStartOfHour(toDateTime(timestamp)) AS hour,
         COUNT(DISTINCT username) AS username_count
    FROM s3('https://tsv9-monitoring-bucket.s3.eu-north-1.amazonaws.com/tenant=*/**')
    GROUP BY tenant, hour
)
SELECT
    tenant,
    CAST(MAX(username_count) AS Int32) AS max_username_count_per_hour
FROM hourly_username_counts
GROUP BY tenant;
