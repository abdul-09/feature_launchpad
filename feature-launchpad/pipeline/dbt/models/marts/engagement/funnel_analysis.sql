{{
  config(
    materialized='table',
    tags=['engagement', 'funnel']
  )
}}

/*
  Funnel Analysis - Step-by-Step Conversion
  
  Tracks user progression through the feature:
  1. Feature View
  2. Quiz Started  
  3. First Question
  4. Mid-Quiz (50%)
  5. Quiz Completed
  6. Result Viewed
  7. Result Shared
  
  Calculates drop-off rates between each step.
*/

with events as (
    select * from {{ ref('fact_events') }}
),

-- Define funnel steps with their order
funnel_steps as (
    select 1 as step_order, 'feature_view' as step_name, 'Feature Viewed' as step_label
    union all select 2, 'quiz_started', 'Quiz Started'
    union all select 3, 'question_1', 'First Question'
    union all select 4, 'mid_quiz', 'Mid-Quiz (50%)'
    union all select 5, 'quiz_completed', 'Quiz Completed'
    union all select 6, 'result_viewed', 'Result Viewed'
    union all select 7, 'result_shared', 'Result Shared'
),

-- Calculate which users reached each step
user_funnel_progress as (
    select
        user_id,
        session_id,
        event_date,
        
        -- Step 1: Feature View
        max(case when event_type in ('feature_view', 'page_view', 'quiz_started') then 1 else 0 end) as reached_feature_view,
        
        -- Step 2: Quiz Started
        max(case when event_type = 'quiz_started' then 1 else 0 end) as reached_quiz_started,
        
        -- Step 3: First Question (step 1)
        max(case when event_type = 'question_viewed' and step_number = 1 then 1 else 0 end) as reached_question_1,
        
        -- Step 4: Mid-Quiz (reached 50% of steps)
        max(case when flow_progress_pct >= 50 then 1 else 0 end) as reached_mid_quiz,
        
        -- Step 5: Quiz Completed
        max(case when event_type = 'quiz_completed' then 1 else 0 end) as reached_quiz_completed,
        
        -- Step 6: Result Viewed
        max(case when event_type = 'result_viewed' then 1 else 0 end) as reached_result_viewed,
        
        -- Step 7: Result Shared
        max(case when event_type = 'result_shared' then 1 else 0 end) as reached_result_shared,
        
        -- Time to complete (for those who completed)
        max(total_time_ms) as completion_time_ms
        
    from events
    group by user_id, session_id, event_date
),

-- Aggregate funnel metrics by date
daily_funnel as (
    select
        event_date,
        
        -- User counts at each step
        count(distinct user_id) as total_users,
        sum(reached_feature_view) as step1_feature_view,
        sum(reached_quiz_started) as step2_quiz_started,
        sum(reached_question_1) as step3_first_question,
        sum(reached_mid_quiz) as step4_mid_quiz,
        sum(reached_quiz_completed) as step5_completed,
        sum(reached_result_viewed) as step6_result_viewed,
        sum(reached_result_shared) as step7_result_shared,
        
        -- Average time to complete
        avg(completion_time_ms) filter (where reached_quiz_completed = 1) as avg_completion_time_ms
        
    from user_funnel_progress
    group by event_date
),

-- Calculate conversion rates
funnel_with_rates as (
    select
        event_date,
        total_users,
        
        -- Step counts
        step1_feature_view,
        step2_quiz_started,
        step3_first_question,
        step4_mid_quiz,
        step5_completed,
        step6_result_viewed,
        step7_result_shared,
        
        -- Conversion rates (from top of funnel)
        round(step1_feature_view::float / nullif(total_users, 0) * 100, 2) as step1_pct,
        round(step2_quiz_started::float / nullif(total_users, 0) * 100, 2) as step2_pct,
        round(step3_first_question::float / nullif(total_users, 0) * 100, 2) as step3_pct,
        round(step4_mid_quiz::float / nullif(total_users, 0) * 100, 2) as step4_pct,
        round(step5_completed::float / nullif(total_users, 0) * 100, 2) as step5_pct,
        round(step6_result_viewed::float / nullif(total_users, 0) * 100, 2) as step6_pct,
        round(step7_result_shared::float / nullif(total_users, 0) * 100, 2) as step7_pct,
        
        -- Step-to-step conversion rates
        round(step2_quiz_started::float / nullif(step1_feature_view, 0) * 100, 2) as conv_1_to_2,
        round(step3_first_question::float / nullif(step2_quiz_started, 0) * 100, 2) as conv_2_to_3,
        round(step4_mid_quiz::float / nullif(step3_first_question, 0) * 100, 2) as conv_3_to_4,
        round(step5_completed::float / nullif(step4_mid_quiz, 0) * 100, 2) as conv_4_to_5,
        round(step6_result_viewed::float / nullif(step5_completed, 0) * 100, 2) as conv_5_to_6,
        round(step7_result_shared::float / nullif(step6_result_viewed, 0) * 100, 2) as conv_6_to_7,
        
        -- Drop-off rates (inverse of conversion)
        round(100 - (step2_quiz_started::float / nullif(step1_feature_view, 0) * 100), 2) as dropoff_1_to_2,
        round(100 - (step3_first_question::float / nullif(step2_quiz_started, 0) * 100), 2) as dropoff_2_to_3,
        round(100 - (step4_mid_quiz::float / nullif(step3_first_question, 0) * 100), 2) as dropoff_3_to_4,
        round(100 - (step5_completed::float / nullif(step4_mid_quiz, 0) * 100), 2) as dropoff_4_to_5,
        
        -- Overall funnel efficiency
        round(step5_completed::float / nullif(step1_feature_view, 0) * 100, 2) as overall_completion_rate,
        round(step7_result_shared::float / nullif(step5_completed, 0) * 100, 2) as share_after_completion_rate,
        
        -- Timing
        round(avg_completion_time_ms / 1000.0, 1) as avg_completion_time_seconds
        
    from daily_funnel
)

select 
    *,
    current_timestamp as calculated_at
from funnel_with_rates
order by event_date desc
