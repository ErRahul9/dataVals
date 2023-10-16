select update_time::date  as date,
       campaign_id,
       percentile_cont(0.5) within group ( order by budget ) as median_daily_budget
from archives.daily_budget_archives dba
where campaign_id = :campaign_id
  and update_time between :start_date and :end_date
group by update_time::date,
         campaign_id
order by update_time::date;



