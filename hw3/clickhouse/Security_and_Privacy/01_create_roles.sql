-- Create two roles
CREATE ROLE IF NOT EXISTS analyst_full;
CREATE ROLE IF NOT EXISTS analyst_limited;

-- Create users (if needed)
CREATE USER IF NOT EXISTS user_full IDENTIFIED BY 'full123';
CREATE USER IF NOT EXISTS user_limited IDENTIFIED BY 'limited123';

-- Grant roles to users
GRANT analyst_full TO user_full;
GRANT analyst_limited TO user_limited;