
select update_time::date as date,
       campaign_id,
       percentile_cont(0.5) within group ( order by threshold )::INTEGER as median_recency_threshold
from archives.recency_score_threshold_archives rsta
where campaign_id = :campaign_id
and update_time >= :start_date
and update_time < :end_date
group by update_time::date, campaign_id
-- order by update_time::date