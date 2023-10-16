 SELECT c.platform_device_type as deviceType,
               HOUR(c.imp_rx_time_utc) AS bid_hour,
               c.line_item_id AS line_item_id,
               count(c.auction_id) as win_counts,
               (SUM(c.win_cost_micros_usd))/1000000 AS hourly_spend
        FROM win_logs c
        WHERE c.time >= :start_time ::timestamp - interval '1' hour
              AND c.time < :end_time ::timestamp + interval '1' hour
              AND c.imp_rx_time_utc >= :start_time
              AND c.imp_rx_time_utc < :end_time
              AND c.line_item_id = :line_item_id
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3