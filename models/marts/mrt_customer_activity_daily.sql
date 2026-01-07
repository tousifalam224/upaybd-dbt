{{ config(materialized='table') }}

select
  f.txn_date,
  f.customer_wallet,
  f.txn_row_count,

  d.region,
  d.tm,
  d.rm,
  d.dso,
  d.dh_code,
  d.agent_wallet,
  d.distributor_wallet,
  d.dso_wallet,
  d.agent_active_status
from {{ ref('int_fct_customer_activity_daily') }} f
left join {{ ref('int_dim_customer') }} d
  on f.customer_wallet = d.customer_wallet
