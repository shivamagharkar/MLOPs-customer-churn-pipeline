with source as (
    select * from RAW.CHURN_CUSTOMERS
)

select
    CUSTOMERID as customer_id,
    AGE,
    GENDER,
    TENURE,
    "Usage Frequency" as usage_frequency,
    "Support Calls" as support_calls,
    "Payment Delay" as payment_delay,
    "Subscription Type" as subscription_type,
    "Contract Length" as contract_length,
    "Total Spend" as total_spend,
    "Last Interaction" as last_interaction,
    CHURN
from source
