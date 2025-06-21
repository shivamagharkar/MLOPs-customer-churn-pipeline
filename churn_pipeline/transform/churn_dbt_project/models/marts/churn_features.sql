with base as (
    select * from {{ ref('stg_churn_customers') }}
)

select
    customer_id,
    cast(age as integer) as age,
    lower(gender) as gender,
    cast(tenure as integer) as tenure,
    cast(usage_frequency as float) as usage_frequency,
    cast(support_calls as integer) as support_calls,
    cast(payment_delay as integer) as payment_delay,
    lower(subscription_type) as subscription_type,

    case lower(contract_length)
        when 'annual' then 12
        when 'monthly' then 1
        else null
    end as contract_length_months,

    cast(total_spend as float) as total_spend,

    cast(last_interaction as integer) as days_since_last_interaction,

    case
        when churn = 1 then 1
        when churn = 0 then 0
        else null
    end as churn_label

from base
where churn is not null
