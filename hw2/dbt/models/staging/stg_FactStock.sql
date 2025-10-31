
    SELECT
        abs(mod(cast(hash(company) as bigint), 1000000000)) AS CompanyKey,       -- temporary, surrogate key, chanes every time
        trading_day,
        abs(mod(cast(hash(ticker_symbol) as bigint), 1000000000)) as TickerKey,
        open_price,
        close_price,
        high_price,
        low_price,
        market_cap,
        dividend
    FROM bronze.stocks_raw;
