select campaign_id,
       hourly_budget,
       device_type_budget_percent,
       advertiser_id,
       line_item_id
from sync.creative_metadata  where campaign_id = :campaign_id