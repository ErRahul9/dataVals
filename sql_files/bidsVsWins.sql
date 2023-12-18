select hour(a.time) as bid_hour,
c.platform_device_type as deviceType,
a.plat
b.steelhouse_id as advertiser_id,count(a.auction_id) as bid_counts ,
count(c.auction_id) as win_counts
from bid_logs a join sync.beeswax_advertiser_mapping b on a.advertiser_id=b.partner_id
left join win_logs c on c.auction_id=a.auction_id and c.time >= :start_time and c.time < :end_time
where a.time >= :start_time and a.time < :end_time
and b.steelhouse_id = :advertiser_id
group by 1 ,2 ,3 order by 1,2,3