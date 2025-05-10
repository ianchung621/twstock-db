WITH large_only AS (
    SELECT flt.*, ci.stock_id
    FROM future_large_trader flt
    JOIN contract_info ci
      ON flt.contract_id = ci.contract_id
    WHERE flt.contract_name NOT LIKE '小型%'
),

calendar AS (
    SELECT generate_series('2004-01-01'::date, CURRENT_DATE, interval '1 day')::timestamp WITHOUT time zone AS trade_date
),
revenue_mapping AS (
    SELECT 
        trade_date,
        -- Revenue revealed on 10th of each month, so subtract 10 days, then floor to month start
        (date_trunc('month', trade_date - interval '9 days')
		- interval '1 month')::date AS available_revenue_date
    FROM calendar
),
revenue_aligned AS (
    SELECT 
        rm.trade_date,
        sr.stock_id,
        sr.revenue
    FROM revenue_mapping rm
    JOIN stock_revenue sr
      ON sr.date = rm.available_revenue_date
)

SELECT 
    l.date,
    l.contract_id,
    l.contract_name,
    l.stock_id,
    
    -- large trader
    l.buy_top5_volume,
    l.sell_top5_volume,
    l.total_volume,
    
    -- stock_ii
    (ii.fii_non_dealer_volume + ii.fii_dealer_volume) AS fii_volume,
    ii.trust_volume,
    (ii.dealer_self_volume + ii.dealer_hedge_volume) AS dealer_volume,

    -- adjusted price
    ap.open,
	ap.high,
    ap.low,
    ap.close,

    -- revenue (mapped to reveal logic)
    ra.revenue

FROM large_only l
LEFT JOIN stock_ii ii
  ON l.date = ii.date AND l.stock_id = ii.stock_id

LEFT JOIN adjusted_price ap
  ON l.date = ap.date AND l.stock_id = ap.stock_id

LEFT JOIN revenue_aligned ra
  ON l.date = ra.trade_date AND l.stock_id = ra.stock_id
ORDER BY date
