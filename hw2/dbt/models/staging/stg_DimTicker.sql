

SELECT
    TickerKey, -- Primary Key
    TicketSymbol,
    Exchange,
    Class,
    Currency,
    ValidFrom,   -- NEW FIELD compared to PR1: as we changes DimCompany to SDC2 then I add ValidFrom and ValidTo dates
    ValidTo   -- NEW FIELD compares to PR1
FROM  file('/var/lib/clickhouse/DimTicker.csv')