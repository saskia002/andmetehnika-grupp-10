CREATE DATABASE IF NOT EXISTS forbes_2000;

CREATE TABLE IF NOT EXISTS forbes_2000.companies
(
	`rank` UInt32,
	`company` String,
	`ticker` String,
	`headquarters` String,
	`industry` String,
	`sales_in_millions` Float64,
	`profit_in_millions` Float64,
	`assets_in_millions` Float64,
	`market_value_in_millions` Float64
)
ENGINE = MergeTree
ORDER BY (`rank`);