{{ config(materialized='view') }}

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
  last_seen_txn_date
from {{ ref('int_dim_customer') }}
