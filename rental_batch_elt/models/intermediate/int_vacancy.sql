with move_in_out as (
    select
        unit_key,
        tenant_id,
        event_type,
        dt_occurred
    from {{ ref('stg_events') }}
    where event_type in ('MOVE_IN', 'MOVE_OUT')
),

tenant_ranges as (
    select
        move_in.unit_key,
        move_in.tenant_id,
        move_in.dt_occurred as dt_movein,
        move_out.dt_occurred as dt_moveout
    from move_in_out move_in
    left join move_in_out move_out
      on move_in.unit_key = move_out.unit_key
     and move_in.tenant_id = move_out.tenant_id
     and move_in.event_type = 'MOVE_IN'
     and move_out.event_type = 'MOVE_OUT'
),

months as (
    select
        dateadd(month, seq4(), '2023-01-01') as month_start
    from table(generator(rowcount => 36))
),

months_calendar as (
    select
        month_start,
        last_day(month_start) as month_end,
        dayofmonth(last_day(month_start)) as calendar_days
    from months
),

occupancy_raw as (
    select
        m.month_start,
        m.month_end,
        m.calendar_days,
        t.unit_key,
        t.tenant_id,
        greatest(m.month_start, t.dt_movein) as occupancy_start,
        least(m.month_end, coalesce(t.dt_moveout, m.month_end)) as occupancy_end
    from months_calendar m
    left join tenant_ranges t
      on m.month_start <= coalesce(t.dt_moveout, current_date)
     and m.month_end >= t.dt_movein
),

occupancy_days as (
    select
        month_start,
        unit_key,
        calendar_days,
        tenant_id,
        datediff(day, occupancy_start, occupancy_end) + 1 as occupancy_days
    from occupancy_raw
),

unit_month_agg as (
    select
        month_start,
        unit_key,
        calendar_days,
        max(occupancy_days) as occupancy_days
    from occupancy_days
    group by 1, 2, 3
)

select
    month_start,
    unit_key,
    calendar_days,
    calendar_days - occupancy_days as vacant_days
from unit_month_agg