select sba.day::date as date,
       sba.advertiser_id,
       c.campaign_id,
       sum(impressions) imps,
       sum(uniques) uniques,
       sum(clicks + views) visits,
       sum(click_conversions + view_conversions) convs
from sum_by_advertiser_by_day sba left outer join  campaigns c
on sba.advertiser_id = c.advertiser_id
where sba.advertiser_id = :advertiser_id
and day >= :start_date
group by sba.day,sba.advertiser_id,c.campaign_id
having c.campaign_id = :campaign_id
order by 1,2,3;




-- select day, sum(impressions) imps,
--        advertiser_id,
--        sum(uniques) uniques,
--        sum(clicks + views) visits,
--        sum(click_conversions + view_conversions) convs
-- from sum_by_advertiser_by_day where advertiser_id = :advertiser_id
--                                 and day >= :start_date group by 1 order by 1;
--
--
