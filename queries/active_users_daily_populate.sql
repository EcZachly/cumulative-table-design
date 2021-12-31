INSERT INTO active_users_daily

SELECT
    user_id,
    -- If the user_id has at least 1 event, they are daily active
    IF(COUNT(user_id) > 0, 1, 0) as is_active_today,
    COUNT(CASE WHEN event_type = 'like' THEN 1 END) as num_likes,
    COUNT(CASE WHEN event_type = 'comment' THEN 1 END) as num_comments,
    COUNT(CASE WHEN event_type = 'share' THEN 1 END) as num_shares,
    CAST('2022-01-01' AS DATE) as snapshot_date
FROM events
WHERE event_date = '2022-01-01'
GROUP BY user_id