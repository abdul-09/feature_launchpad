{{
  config(
    materialized='table',
    tags=['core', 'facts']
  )
}}

/*
  Fact table for all user events
  
  This is the primary fact table containing all user interactions
  with the feature, enriched with derived metrics.
*/

with events as (
    select * from {{ ref('stg_events') }}
),

enriched as (
    select
        -- Primary key
        event_id,
        
        -- Foreign keys
        user_id,
        session_id,
        
        -- Event attributes
        event_type,
        feature_name,
        
        -- Timestamps
        event_timestamp,
        event_date,
        event_hour,
        processed_at,
        
        -- Event categorization
        case 
            when event_type in ('page_view', 'feature_view') then 'view'
            when event_type in ('quiz_started', 'quiz_step_completed', 'quiz_completed', 'quiz_abandoned') then 'flow'
            when event_type in ('slider_adjusted', 'option_selected', 'option_deselected', 'question_viewed') then 'interaction'
            when event_type in ('result_viewed', 'result_shared', 'result_saved') then 'outcome'
            when event_type in ('cta_clicked', 'link_clicked') then 'engagement'
            when event_type in ('session_start', 'session_end') then 'session'
            else 'other'
        end as event_category,
        
        -- Flow position
        step_number,
        total_steps,
        case 
            when total_steps > 0 then round(step_number::float / total_steps * 100, 1)
            else null
        end as flow_progress_pct,
        
        -- Interaction metrics
        slider_value,
        previous_value,
        case 
            when slider_value is not null and previous_value is not null 
            then abs(slider_value - previous_value)
            else null
        end as slider_change_magnitude,
        option_id,
        option_label,
        
        -- Outcome metrics
        recommendation_id,
        recommendation_name,
        match_score,
        share_platform,
        
        -- Timing metrics
        time_on_step_ms,
        total_time_ms,
        case 
            when time_on_step_ms > 0 then round(time_on_step_ms / 1000.0, 2)
            else null
        end as time_on_step_seconds,
        
        -- Device/context
        device_type,
        page_path,
        referrer,
        user_timezone,
        user_locale,
        
        -- Screen metrics
        screen_width,
        screen_height,
        case 
            when screen_width > 0 and screen_height > 0 
            then round(screen_width::float / screen_height, 2)
            else null
        end as screen_aspect_ratio
        
    from events
)

select * from enriched
