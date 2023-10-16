select t.campaign_id ,q.total_media_cost as spend_25, t.total_media_cost as spend_26, p.total_media_cost as spend_27 , r.total_media_cost as spend_28 , s.total_media_cost as spend_29 from
(select hour::date as date, campaign_id, sum(media_cost) as total_media_cost
from summarydata.all_facts af
  where  hour::date = '2023-09-26'
  group by hour::date, campaign_id
) t
inner join (select hour::date as date, campaign_id, sum(media_cost) as total_media_cost
from summarydata.all_facts af
  where  hour = '2023-09-27'
group by hour::date, campaign_id
order by hour::date
) p
on t.campaign_id = p.campaign_id
inner join (select hour::date as date, campaign_id, sum(media_cost) as total_media_cost
from summarydata.all_facts af
  where  hour = '2023-09-28'
group by hour::date, campaign_id
order by hour::date ) r
on t.campaign_id = r.campaign_id
inner join (select hour::date as date, campaign_id, sum(media_cost) as total_media_cost
from summarydata.all_facts af
  where  hour = '2023-09-29'
group by hour::date, campaign_id
order by hour::date ) s
on t.campaign_id = s.campaign_id
inner join (select hour::date as date, campaign_id, sum(media_cost) as total_media_cost
from summarydata.all_facts af
  where  hour = '2023-09-25'
group by hour::date, campaign_id
order by hour::date ) q
on t.campaign_id = q.campaign_id