#!/bin/bash
# This script ensures users exist in ClickHouse
# It runs every time the container starts

set -e

CLICKHOUSE_CLIENT="clickhouse-client --host localhost --port 9000"

# Wait for ClickHouse to be ready
echo "Waiting for ClickHouse to be ready..."
until $CLICKHOUSE_CLIENT --query "SELECT 1" > /dev/null 2>&1; do
    echo "ClickHouse is not ready yet, waiting..."
    sleep 2
done

echo "ClickHouse is ready. Ensuring users exist..."

ensure_user() {
    local username=$1
    local password=$2
    local grants=$3
    
    echo "Checking if user '$username' exists..."
    
    USER_EXISTS=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.users WHERE name = '$username'" 2>/dev/null || echo "0")
    
    if [ "$USER_EXISTS" = "0" ] || [ -z "$USER_EXISTS" ]; then
        echo "User '$username' does not exist. Creating..."
        $CLICKHOUSE_CLIENT --query "CREATE USER IF NOT EXISTS $username IDENTIFIED BY '$password'" || {
            echo "Attempting to drop and recreate user '$username'..."
            $CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS $username" 2>/dev/null || true
            $CLICKHOUSE_CLIENT --query "CREATE USER $username IDENTIFIED BY '$password'"
        }
        echo "User '$username' created successfully."
    else
        echo "User '$username' already exists."
        $CLICKHOUSE_CLIENT --query "ALTER USER $username IDENTIFIED BY '$password'" 2>/dev/null || true
    fi
    
    echo "Setting permissions for user '$username'..."
    $CLICKHOUSE_CLIENT --query "$grants" || echo "Warning: Some permissions may not have been set (user might already have them)"
}

ensure_user "etl" "pass" "GRANT SELECT, INSERT, CREATE, CREATE DATABASE ON *.* TO etl"

ensure_user "dbt_user" "dbt_pass" "GRANT SELECT, INSERT, CREATE, CREATE DATABASE, ALTER ON *.* TO dbt_user"

echo "User verification complete!"

