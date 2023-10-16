select HOUR(update_time)  as bid_hour,
       campaign_id,
       percentile_cont(0.5) within group ( order by budget ) as median_hourly_budget
from archives.daily_budget_archives dba
where campaign_id = :campaign_id
  and update_time between :start_time and :end_time
group by update_time,
         campaign_id
order by update_time;