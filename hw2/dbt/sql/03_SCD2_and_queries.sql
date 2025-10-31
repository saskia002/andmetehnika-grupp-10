
-- UPDATING SCD2 in case change is detected or new value comes or value dissappears from list 

-- if there is a row in staging that does not match the latest version in DimCompany
-- insert a new row with ValidFrom = today, ValidTo = '9999-12-31'
-- update the old row to set ValidTo = yesterday

-- DimCustomer changes

-- SET validiy date to previous day
ALTER TABLE Forbes_2000.DimCompany
  UPDATE ValidTo = toDate('2025-09-20') -- TOUPDATE to today-1??
  WHERE CustomerID = 1 AND ValidTo = toDate('9999-12-31');

-- INSERT new value(s)
INSERT INTO supermarket.DimCustomer (
  CustomerKey, CustomerID, FirstName, LastName, Segment, City, ValidFrom, ValidTo
) VALUES (
  3, 1, 'Alice', 'Smith', 'Regular', 'Tartu', toDate('2025-09-21'), toDate('9999-12-31')
);

-- DimTicker changes

-- SET validiy date to previous day
ALTER TABLE Forbes_2000.DimTicker
  UPDATE ValidTo = toDate('2025-09-20') -- TOUPDATE to today-1??
  WHERE CustomerID = 1 AND ValidTo = toDate('9999-12-31');

-- INSERT new value(s)
INSERT INTO supermarket.DimCustomer (
  CustomerKey, CustomerID, FirstName, LastName, Segment, City, ValidFrom, ValidTo
) VALUES (
  3, 1, 'Alice', 'Smith', 'Regular', 'Tartu', toDate('2025-09-21'), toDate('9999-12-31')
);

