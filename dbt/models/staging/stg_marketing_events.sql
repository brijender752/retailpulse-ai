select
    cast(marketing_event_id as bigint) marketing_event_id,
    campaign_id,
    cast(customer_id as bigint) customer_id,
    campaign,
    channel,
    -- Convert Boolean flags to numbers before applying the numeric null default.
    coalesce(cast(impression as bigint), 0) impression,
    coalesce(cast(click as bigint), 0) click,
    coalesce(cast(conversion as bigint), 0) conversion,
    cast(coalesce(cost, 0) as decimal(18,2)) cost,
    {{ cdc_timestamp('event_timestamp') }} event_timestamp
from {{ source('silver','marketing_events') }}
