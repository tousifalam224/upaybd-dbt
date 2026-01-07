{{ config(materialized='table') }}

with base as (
    select
        customer_wallet,
        agent_wallet,
        distributor_wallet,
        dso_wallet,
        region,
        tm,
        rm,
        dso,
        dh_code,
        agent_active_status,
        txn_date::date as txn_date
    from {{ ref('stg_customer') }}
    where customer_wallet is not null
),

ranked as (
    select
        *,
        row_number() over (
            partition by customer_wallet
            order by txn_date desc
        ) as rn
    from base
)

select
    customer_wallet,
    agent_wallet,
    distributor_wallet,
    dso_wallet,
    region,
    tm,
    rm,
    dso,
    dh_code,
    agent_active_status,
    txn_date as last_seen_txn_date
from ranked
where rn = 1
