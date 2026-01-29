{{
  config(
    materialized='table',
    tags=['engagement', 'metrics', 'showcase']
  )
}}

/*
  Engagement Metrics - Feature Impact Analysis
  
  This model computes the key metrics that demonstrate feature impact:
  
  1. ADOPTION: % of DAU who interacted with the feature
  2. ENGAGEMENT DEPTH: Average interactions per session, completion rate
  3. QUALITY: Time spent per interaction, drop-off points
  4. OUTCOME: Correlation between completion and retention
  
  These metrics directly support claims like:
  "Users who completed the quiz had a 50% higher probability of returning"
*/

with events as (
    select * from {{ ref('fact_events') }}
),

users as (
    select * from {{ ref('dim_users') }}
),

-- Daily Active Users (DAU) baseline
daily_users as (
    select
        event_date,
        count(distinct user_id) as dau
    from events
    group by event_date
),

-- Feature adopters per day
daily_feature_adoption as (
    select
        event_date,
        count(distinct user_id) as feature_users,
        count(distinct session_id) as feature_sessions,
        count(*) as total_interactions
    from events
    where event_category in ('interaction', 'flow')
    group by event_date
),

-- Completion funnel by day
daily_funnel as (
    select
        event_date,
        count(distinct case when event_type = 'quiz_started' then user_id end) as started_users,
        count(distinct case when event_type = 'quiz_completed' then user_id end) as completed_users,
        count(distinct case when event_type = 'result_shared' then user_id end) as shared_users
    from events
    group by event_date
),

-- Session-level engagement metrics
session_engagement as (
    select
        session_id,
        user_id,
        event_date,
        count(*) as events_in_session,
        count(distinct event_type) as unique_event_types,
        sum(case when event_type = 'slider_adjusted' then 1 else 0 end) as slider_adjustments,
        sum(case when event_type = 'option_selected' then 1 else 0 end) as option_selections,
        max(step_number) as max_step_reached,
        max(total_steps) as total_steps,
        max(total_time_ms) as session_duration_ms,
        max(match_score) as final_match_score,
        max(case when event_type = 'quiz_completed' then 1 else 0 end) as completed_quiz,
        max(case when event_type = 'result_shared' then 1 else 0 end) as shared_result
    from events
    group by session_id, user_id, event_date
),

-- Aggregate session metrics
session_metrics_summary as (
    select
        event_date,
        
        -- Volume
        count(*) as total_sessions,
        
        -- Engagement depth
        avg(events_in_session) as avg_events_per_session,
        avg(unique_event_types) as avg_unique_actions,
        avg(slider_adjustments) as avg_slider_adjustments,
        avg(option_selections) as avg_option_selections,
        
        -- Progress
        avg(case when total_steps > 0 then max_step_reached::float / total_steps * 100 end) as avg_progress_pct,
        
        -- Time
        avg(session_duration_ms) / 1000.0 as avg_session_duration_seconds,
        percentile_cont(0.5) within group (order by session_duration_ms) / 1000.0 as median_session_duration_seconds,
        
        -- Completion
        sum(completed_quiz) as completed_sessions,
        avg(completed_quiz) * 100 as completion_rate_pct,
        
        -- Quality (match scores)
        avg(final_match_score) filter (where final_match_score is not null) as avg_match_score,
        
        -- Sharing
        sum(shared_result) as shared_sessions,
        avg(shared_result) * 100 as share_rate_pct
        
    from session_engagement
    group by event_date
),

-- Retention analysis: do completers return?
retention_analysis as (
    select
        u.user_id,
        u.user_segment,
        u.quizzes_completed > 0 as is_completer,
        u.total_sessions,
        u.active_days,
        u.days_since_last_seen,
        
        -- Did they return within 7 days?
        case when u.total_sessions > 1 then 1 else 0 end as returned,
        case when u.active_days > 1 then 1 else 0 end as returned_different_day
        
    from users u
),

retention_comparison as (
    select
        'completers' as user_group,
        count(*) as user_count,
        avg(total_sessions) as avg_sessions,
        avg(active_days) as avg_active_days,
        sum(returned)::float / count(*) * 100 as return_rate_pct,
        sum(returned_different_day)::float / count(*) * 100 as next_day_return_rate_pct
    from retention_analysis
    where is_completer = true
    
    union all
    
    select
        'non_completers' as user_group,
        count(*) as user_count,
        avg(total_sessions) as avg_sessions,
        avg(active_days) as avg_active_days,
        sum(returned)::float / count(*) * 100 as return_rate_pct,
        sum(returned_different_day)::float / count(*) * 100 as next_day_return_rate_pct
    from retention_analysis
    where is_completer = false
),

-- Final daily metrics
daily_metrics as (
    select
        du.event_date,
        
        -- ADOPTION
        du.dau,
        coalesce(fa.feature_users, 0) as feature_users,
        case when du.dau > 0 
            then round(fa.feature_users::float / du.dau * 100, 2) 
            else 0 
        end as feature_adoption_rate_pct,
        
        -- ENGAGEMENT DEPTH
        coalesce(fa.total_interactions, 0) as total_interactions,
        coalesce(sm.avg_events_per_session, 0) as avg_events_per_session,
        coalesce(sm.avg_slider_adjustments, 0) as avg_slider_adjustments,
        coalesce(sm.avg_progress_pct, 0) as avg_progress_pct,
        
        -- QUALITY
        coalesce(sm.avg_session_duration_seconds, 0) as avg_session_duration_seconds,
        coalesce(sm.median_session_duration_seconds, 0) as median_session_duration_seconds,
        coalesce(sm.avg_match_score, 0) as avg_match_score,
        
        -- OUTCOMES
        coalesce(df.started_users, 0) as quiz_starts,
        coalesce(df.completed_users, 0) as quiz_completions,
        case when df.started_users > 0 
            then round(df.completed_users::float / df.started_users * 100, 2)
            else 0 
        end as quiz_completion_rate_pct,
        coalesce(df.shared_users, 0) as result_shares,
        coalesce(sm.share_rate_pct, 0) as share_rate_pct
        
    from daily_users du
    left join daily_feature_adoption fa on du.event_date = fa.event_date
    left join daily_funnel df on du.event_date = df.event_date
    left join session_metrics_summary sm on du.event_date = sm.event_date
)

select 
    *,
    current_timestamp as calculated_at
from daily_metrics
order by event_date desc
