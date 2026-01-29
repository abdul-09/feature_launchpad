{{
  config(
    materialized='view',
    tags=['staging', 'events']
  )
}}

/*
  Staging model for raw events
  
  This model:
  - Reads from the raw Parquet files written by Spark
  - Applies basic data type conversions
  - Filters invalid records
  - Adds derived fields for downstream use
*/

with raw_events as (
    select * from read_parquet('/data/events/raw/**/*.parquet')
),

cleaned as (
    select
        -- Identifiers
        event_id,
        user_id,
        session_id,
        
        -- Event details
        event_type,
        feature_name,
        
        -- Timestamps
        cast(event_timestamp as timestamp) as event_timestamp,
        cast(processed_at as timestamp) as processed_at,
        event_date,
        cast(event_hour as integer) as event_hour,
        
        -- Event properties (flattened)
        event_properties.question_id as question_id,
        event_properties.step_number as step_number,
        event_properties.total_steps as total_steps,
        event_properties.slider_value as slider_value,
        event_properties.previous_value as previous_value,
        event_properties.option_id as option_id,
        event_properties.option_label as option_label,
        event_properties.selected_options as selected_options,
        event_properties.recommendation_id as recommendation_id,
        event_properties.recommendation_name as recommendation_name,
        event_properties.match_score as match_score,
        event_properties.time_on_step_ms as time_on_step_ms,
        event_properties.total_time_ms as total_time_ms,
        event_properties.share_platform as share_platform,
        event_properties.cta_id as cta_id,
        event_properties.cta_label as cta_label,
        
        -- Context (flattened)
        context.page_url as page_url,
        context.page_path as page_path,
        context.referrer as referrer,
        context.device_type as device_type,
        context.screen_width as screen_width,
        context.screen_height as screen_height,
        context.timezone as user_timezone,
        context.locale as user_locale
        
    from raw_events
    where 
        -- Filter invalid records
        event_id is not null
        and user_id is not null
        and session_id is not null
        and event_type is not null
        
        -- Filter bot traffic (if configured)
        {% if not var('include_bot_traffic') %}
        and (
            context.user_agent is null 
            or context.user_agent not like '%bot%'
        )
        {% endif %}
)

select * from cleaned
