INSERT INTO active_users_cumulated

-- First read in yesterday from the cumulated table
WITH yesterday AS (
    SELECT * FROM active_users_cumulated
    WHERE snapshot_date = '2021-12-31'
),
-- Read in the daily active user numbers for just today from the daily table
today AS (
    SELECT * FROM active_users_daily
    WHERE snapshot_date = '2022-01-01'
),

-- we FULL OUTER JOIN today and yesterday. We need to do some COALESCE both because
-- activity_array may not exist yet for a given user (i.e. they are brand new)
-- is_active_today may be null as well since it's null on days when a user didn't generate an event
combined AS (
SELECT
 -- We need to COALESCE here since t.user_id and y.user_id may be
 COALESCE(y.user_id, t.user_id) AS user_id,
 COALESCE(
        IF(CARDINALITY( y.activity_array) < 30,
            ARRAY[COALESCE(t.is_active_today, 0)] || y.activity_array,
            ARRAY[COALESCE(t.is_active_today, 0)] || SLICE(y.activity_array, -1, 29)
         )
       , ARRAY[t.is_active_today]
 ) as activity_array,
 t.snapshot_date
 FROM yesterday y
            FULL OUTER JOIN today t
            ON y.user_id = t.user_id
)

SELECT
      user_id,
      activity_array[1] AS is_daily_active,
      -- if any of the array values are 1, then the user was active in the last month
      CASE WHEN ARRAY_SUM(activity_array) > 0 THEN 1 ELSE 0 END AS is_monthly_active,
      -- if any of the first 7 array values are non-zero, then the user was active in the last week
      CASE WHEN ARRAY_SUM(SLICE(activity_array, 1, 7)) > 0 THEN 1 ELSE 0 END AS is_weekly_active
      activity_array,

FROM combined





