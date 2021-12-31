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
 -- if y.activity_array is null (indicating a brand new user), we have to coalesce with an array of size 1
 -- this array just holds the value for today since that's the only history we have
 COALESCE(
        IF(CARDINALITY( y.activity_array) < 30,
            ARRAY[COALESCE(t.is_active_today, 0)] || y.activity_array,
            ARRAY[COALESCE(t.is_active_today, 0)] || SLICE(y.activity_array, -1, 29)
         )
       , ARRAY[t.is_active_today]
 ) as activity_array,
  COALESCE(
         IF(CARDINALITY( y.like_array) < 30,
             ARRAY[COALESCE(t.num_likes, 0)] || y.like_array,
             ARRAY[COALESCE(t.num_likes, 0)] || SLICE(y.like_array, -1, 29)
          )
        , ARRAY[t.num_likes]
  ) as like_array,
    COALESCE(
           IF(CARDINALITY( y.comment_array) < 30,
               ARRAY[COALESCE(t.num_comments, 0)] || y.comment_array,
               ARRAY[COALESCE(t.num_comments, 0)] || SLICE(y.comment_array, -1, 29)
            )
          , ARRAY[t.num_comments]
    ) as comment_array,
      COALESCE(
             IF(CARDINALITY( y.share_array) < 30,
                 ARRAY[COALESCE(t.num_shares, 0)] || y.share_array,
                 ARRAY[COALESCE(t.num_shares, 0)] || SLICE(y.share_array, -1, 29)
              )
            , ARRAY[t.num_shares]
      ) as share_array,
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
      like_array,
      share_array,
      comment_array,
      ARRAY_SUM(SLICE(like_array, 1, 7)) as num_likes_7d,
      ARRAY_SUM(SLICE(comment_array, 1, 7)) as num_comments_7d,
      ARRAY_SUM(SLICE(share_array, 1, 7)) as num_shares_7d,
      ARRAY_SUM(like_array) as num_likes_30d,
      ARRAY_SUM(comment_array) as num_comments_30d,
      ARRAY_SUM(share_array) as num_shares_30d,
      snapshot_date
FROM combined





