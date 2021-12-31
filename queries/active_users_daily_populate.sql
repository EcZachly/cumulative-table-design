INSERT INTO active_users_daily

SELECT
    user_id,
    -- If the user_id has at least 1 event, they are daily active
    IF(COUNT(user_id) > 0, 1, 0) as is_daily_active
FROM events
WHERE event_date = '2022-01-01'
GROUP BY user_id