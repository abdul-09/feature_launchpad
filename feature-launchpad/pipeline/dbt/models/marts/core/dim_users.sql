{{
  config(
    materialized='table',
    tags=['core', 'dimensions']
  )
}}

/*
  Dimension table for users
  
  Aggregates user-level attributes and lifetime metrics
  from their event history.
*/

with events as (
    select * from {{ ref('fact_events') }}
),

user_metrics as (
    select
        user_id,
        
        -- First/last seen
        min(event_timestamp) as first_seen_at,
        max(event_timestamp) as last_seen_at,
        
        -- Session counts
        count(distinct session_id) as total_sessions,
        count(distinct event_date) as active_days,
        
        -- Event counts
        count(*) as total_events,
        sum(case when event_category = 'interaction' then 1 else 0 end) as interaction_events,
        sum(case when event_category = 'flow' then 1 else 0 end) as flow_events,
        
        -- Quiz completion
        sum(case when event_type = 'quiz_started' then 1 else 0 end) as quizzes_started,
        sum(case when event_type = 'quiz_completed' then 1 else 0 end) as quizzes_completed,
        
        -- Engagement
        sum(case when event_type = 'result_shared' then 1 else 0 end) as results_shared,
        sum(case when event_type = 'cta_clicked' then 1 else 0 end) as ctas_clicked,
        
        -- Device preference
        mode() within group (order by device_type) as primary_device,
        
        -- Timezone
        mode() within group (order by user_timezone) as primary_timezone,
        
        -- Average metrics
        avg(match_score) filter (where match_score is not null) as avg_match_score,
        avg(total_time_ms) filter (where total_time_ms is not null) as avg_completion_time_ms
        
    from events
    group by user_id
),

final as (
    select
        user_id,
        first_seen_at,
        last_seen_at,
        
        -- Tenure
        date_diff('day', first_seen_at, last_seen_at) as user_tenure_days,
        date_diff('day', first_seen_at, current_timestamp) as days_since_first_seen,
        date_diff('day', last_seen_at, current_timestamp) as days_since_last_seen,
        
        -- Activity metrics
        total_sessions,
        active_days,
        total_events,
        interaction_events,
        flow_events,
        
        -- Derived activity metrics
        round(total_events::float / nullif(total_sessions, 0), 1) as avg_events_per_session,
        round(total_sessions::float / nullif(active_days, 0), 2) as sessions_per_active_day,
        
        -- Quiz metrics
        quizzes_started,
        quizzes_completed,
        case 
            when quizzes_started > 0 
            then round(quizzes_completed::float / quizzes_started * 100, 1)
            else 0
        end as quiz_completion_rate,
        
        -- Engagement metrics
        results_shared,
        ctas_clicked,
        
        -- User classification
        case
            when quizzes_completed >= 3 and results_shared > 0 then 'power_user'
            when quizzes_completed >= 1 then 'engaged'
            when quizzes_started >= 1 then 'explorer'
            else 'visitor'
        end as user_segment,
        
        -- Device and context
        primary_device,
        primary_timezone,
        
        -- Quality metrics
        avg_match_score,
        avg_completion_time_ms,
        
        -- Metadata
        current_timestamp as updated_at
        
    from user_metrics
)

select * from final
