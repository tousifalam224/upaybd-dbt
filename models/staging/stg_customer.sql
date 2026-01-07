with source as (

    select *
   -- from public.customer
    from {{ source('raw', 'customer') }}

)

select *
from source
 