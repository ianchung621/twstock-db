### Table: adjusted_price
  - _date_, _stock_id_, open, high, low, close

### Table: broker_info
  - _broker_id_, broker_name, broker_group_query_str, broker_query_str

### Table: broker_transaction
  - _date_, _stock_id_, _broker_id_, volume, turnover

### Table: contract_info
  - _contract_id_, stock_id, stock_name

### Table: future_large_trader
  - _date_, _contract_id_, contract_name, buy_top5_volume, buy_top5_ratio, buy_top5_ii_ratio, buy_top10_volume, buy_top10_ratio, buy_top10_ii_ratio, sell_top5_volume, sell_top5_ratio, sell_top5_ii_ratio, sell_top10_volume, sell_top10_ratio, sell_top10_ii_ratio, total_volume

### Table: index_price
  - _date_, open, high, low, close, volume

### Table: stock_cap_reduction
  - _date_, _stock_id_, adjustment_factor, reduction_close, reduction_ref_price, open_ref_price, reduction_reason

### Table: stock_dividend
  - _date_, _stock_id_, adjustment_factor, ex_div_close, open_ref_price, dividend_value, div_ref_price, dividend_type

### Table: stock_info
  - _stock_id_, stock_name, market_type, industry_type, asset_type, listing_date

### Table: stock_price
  - _date_, _stock_id_, open, high, low, close, volume, turnover, transactions_number

### Table: stock_revenue
  - _date_, _stock_id_, revenue

### Table: stock_split
  - _date_, _stock_id_, adjustment_factor, split_close, split_ref_price, open_ref_price

### Foreign Key Relationships