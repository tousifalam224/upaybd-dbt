{{ config(materialized='table') }}

with base as (
    select
        txn_date::date as txn_date,
        customer_wallet,
        agent_wallet,
        distributor_wallet,
        dso_wallet,
        region,
        tm,
        rm,
        dso,
        dh_code,
        agent_active_status
    from {{ ref('stg_customer') }}
    where txn_date is not null
      and customer_wallet is not null
)

select
    txn_date,
    customer_wallet,

    -- keep latest-ish attributes within the day
    max(agent_wallet) as agent_wallet,
    max(distributor_wallet) as distributor_wallet,
    max(dso_wallet) as dso_wallet,
    max(region) as region,
    max(tm) as tm,
    max(rm) as rm,
    max(dso) as dso,
    max(dh_code) as dh_code,
    max(agent_active_status) as agent_active_status,

    count(*) as txn_row_count
from base
group by 1, 2
