-- =============================================================================
-- Athena queries for the EV fleet telemetry table.
--
-- Assumes the Glue Crawler created database `ev_fleet_db` and table `ev_telemetry`
-- (the table name comes from the S3 folder raw/ev_telemetry/).
-- Partition columns year/month/day/hour are strings; always filter on them so
-- Athena only scans the partitions you need (you pay per TB scanned).
-- The queries look at a rolling 24-hour window rather than "today", because the
-- UTC date rolls over in the afternoon/evening in US time zones.
--
-- Notes:
--   * "timestamp" is a reserved word in Athena, so it must be double-quoted.
--   * All times are UTC (the producer writes UTC and the consumer partitions by UTC).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 0. Sanity check: did data arrive, and which partitions exist?
-- -----------------------------------------------------------------------------
SELECT year, month, day, hour, COUNT(*) AS events
FROM ev_fleet_db.ev_telemetry
GROUP BY year, month, day, hour
ORDER BY year DESC, month DESC, day DESC, hour DESC
LIMIT 24;


-- -----------------------------------------------------------------------------
-- 1. Battery overheating: which vehicles went above 45 °C in the last 24 hours,
--    how often, how hot, and what were they doing at the time?
--    (EV-007 and EV-014 have a simulated faulty cooling system.)
-- -----------------------------------------------------------------------------
SELECT
    vehicle_id,
    COUNT(*)                                          AS overheat_readings,
    MAX(battery_temp_c)                               AS max_temp_c,
    ROUND(AVG(battery_temp_c), 1)                     AS avg_overheat_temp_c,
    MIN(from_iso8601_timestamp("timestamp"))          AS first_seen_utc,
    MAX(from_iso8601_timestamp("timestamp"))          AS last_seen_utc,
    array_join(array_agg(DISTINCT charging_status), ', ') AS status_while_overheating
FROM ev_fleet_db.ev_telemetry
WHERE from_iso8601_timestamp("timestamp") >= current_timestamp - INTERVAL '24' HOUR
  -- partition filter: only today's and yesterday's (UTC) folders are read
  AND concat(year, '-', month, '-', day) >= date_format(current_date - INTERVAL '1' DAY, '%Y-%m-%d')
  AND battery_temp_c > 45
GROUP BY vehicle_id
ORDER BY overheat_readings DESC;


-- -----------------------------------------------------------------------------
-- 2. Charging-station overload: per station, how many minutes in the last 24 hours
--    had more vehicles charging than the station has ports?
--    Port counts mirror STATIONS in src/producer.py.
-- -----------------------------------------------------------------------------
WITH station_capacity (station_id, station_name, ports) AS (
    VALUES
        ('ST-BOS-01', 'Back Bay Garage',  4),
        ('ST-BOS-02', 'Fenway Lot',       2),
        ('ST-BOS-03', 'Seaport Hub',      6),
        ('ST-BOS-04', 'Roxbury Crossing', 2),
        ('ST-CAM-01', 'Kendall Square',   2),
        ('ST-SOM-01', 'Assembly Row',     4)
),
charging_per_minute AS (
    SELECT
        station_id,
        date_trunc('minute', from_iso8601_timestamp("timestamp")) AS minute_utc,
        COUNT(DISTINCT vehicle_id)                                AS vehicles_charging
    FROM ev_fleet_db.ev_telemetry
    WHERE from_iso8601_timestamp("timestamp") >= current_timestamp - INTERVAL '24' HOUR
      -- partition filter: only today's and yesterday's (UTC) folders are read
      AND concat(year, '-', month, '-', day) >= date_format(current_date - INTERVAL '1' DAY, '%Y-%m-%d')
      AND charging_status IN ('CHARGING', 'FAST_CHARGING')
      AND station_id IS NOT NULL
    GROUP BY 1, 2
)
SELECT
    c.station_id,
    s.station_name,
    s.ports,
    COUNT(*)                                             AS active_minutes,
    COUNT_IF(c.vehicles_charging > s.ports)              AS overloaded_minutes,
    MAX(c.vehicles_charging)                             AS peak_vehicles,
    ROUND(100.0 * MAX(c.vehicles_charging) / s.ports)    AS peak_utilization_pct,
    CASE WHEN MAX(c.vehicles_charging) > s.ports THEN 'OVERLOADED' ELSE 'OK' END AS status
FROM charging_per_minute c
JOIN station_capacity s ON c.station_id = s.station_id
GROUP BY c.station_id, s.station_name, s.ports
ORDER BY overloaded_minutes DESC, peak_utilization_pct DESC;


-- -----------------------------------------------------------------------------
-- 3. Fleet snapshot: the latest reading (within the last 24 hours) for every
--    vehicle, lowest battery first.
--    Useful as a "live" dashboard table (who needs to charge right now?).
-- -----------------------------------------------------------------------------
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY vehicle_id
                           ORDER BY from_iso8601_timestamp("timestamp") DESC) AS rn
    FROM ev_fleet_db.ev_telemetry
    WHERE from_iso8601_timestamp("timestamp") >= current_timestamp - INTERVAL '24' HOUR
      -- partition filter: only today's and yesterday's (UTC) folders are read
      AND concat(year, '-', month, '-', day) >= date_format(current_date - INTERVAL '1' DAY, '%Y-%m-%d')
)
SELECT
    vehicle_id,
    from_iso8601_timestamp("timestamp") AS last_seen_utc,
    battery_level,
    battery_temp_c,
    speed_mph,
    charging_status,
    station_id,
    location.lat AS lat,
    location.lon AS lon,
    CASE
        WHEN battery_temp_c > 45 THEN 'OVERHEATING'
        WHEN battery_level < 20  THEN 'LOW_BATTERY'
        ELSE 'OK'
    END AS alert
FROM ranked
WHERE rn = 1
ORDER BY battery_level ASC;


-- -----------------------------------------------------------------------------
-- Bonus: does fast charging run batteries hotter? Average temperature and
-- overheat rate by charging status over the last 24 hours.
-- -----------------------------------------------------------------------------
SELECT
    charging_status,
    COUNT(*)                                                   AS readings,
    ROUND(AVG(battery_temp_c), 1)                              AS avg_temp_c,
    ROUND(100.0 * COUNT_IF(battery_temp_c > 45) / COUNT(*), 2) AS overheat_pct
FROM ev_fleet_db.ev_telemetry
WHERE from_iso8601_timestamp("timestamp") >= current_timestamp - INTERVAL '24' HOUR
  -- partition filter so Athena doesn't scan all history (today and yesterday)
  AND concat(year, '-', month, '-', day) >= date_format(current_date - INTERVAL '1' DAY, '%Y-%m-%d')
GROUP BY charging_status
ORDER BY avg_temp_c DESC;
