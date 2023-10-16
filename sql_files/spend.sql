select i.campaign_id,
       i.time::date as date,
       sum(impression_win_price_micros_usd::numeric/1000000) as media_cost
from (
  select distinct on (impression_auction_id_str) impression_auction_id_str, impression_win_price_micros_usd, bcm.steelhouse_id as campaign_id,
     time :: date as time
  from beeswax_win_logs bwl
  join sync.beeswax_campaign_mapping bcm on bcm.line_item_id = bwl.impression_adgroup_id_line_item_id
  where time >= :start_date and time < :end_date
  and bcm.steelhouse_id in (
:campaign_id
  )
  )i group by 1,2 order by 1 ,2;



-- select i.campaign_id,
--        i.time::date,
--        sum(impression_win_price_micros_usd::numeric/1000000) as media_cost
-- from (
--   select distinct on (impression_auction_id_str) impression_auction_id_str, impression_win_price_micros_usd, bcm.steelhouse_id as campaign_id,
--      time :: date as time
--   from beeswax_win_logs bwl
--   join sync.beeswax_campaign_mapping bcm on bcm.line_item_id = bwl.impression_adgroup_id_line_item_id
--   where time >= '2023-10-01' and time < '2023-10-03'
--   and bcm.steelhouse_id in (
-- 214033
--   )
--   )i group by 1,2 order by 1 ,2;