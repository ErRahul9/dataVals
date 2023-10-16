select update_time::date as date,
       campaign_id,
       percentile_cont(0.5) within group ( order by duration ) as median_fcap_duration
from archives.frequency_cap_archives fca
where campaign_id = :campaign_id
  and update_time >=:start_date
group by update_time::date , campaign_id
order by update_time::date;


