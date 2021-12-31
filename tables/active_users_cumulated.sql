create table active_users_cumulated (
    user_id: integer,
    is_daily_active: integer,
    is_weekly_active: integer,
    is_monthly_active: integer,
    activity_array: array<integer>,
    snapshot_date: date
)

