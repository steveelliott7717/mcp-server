-- Source: professional_profile.user_profile
with source as (select * from {{ ref('profile__user_profile') }})
select
    id::bigint            as user_profile_id,
    date_of_birth::date   as date_of_birth,
    sex::varchar          as sex,
    height_in::numeric    as height_in
from source
