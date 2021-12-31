create table active_users_cumulated (
    user_id: integer,
    is_daily_active: integer,
    is_weekly_active: integer,
    is_monthly_active: integer,
    activity_array: array(integer),
    like_array: array(integer),
    share_array: array(integer),
    comment_array: array(integer),
    num_likes_7d: integer,
    num_comments_7d: integer,
    num_shares_7d: integer,
    num_likes_30d: integer,
    num_comments_30d: integer,
    num_shares_30d: integer,
    snapshot_date: date
)

