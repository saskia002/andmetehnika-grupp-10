from typing import List, Dict, Any, Optional, Tuple
from pymongo import MongoClient
import requests
import json
import os
import argparse


MONGO_URI = "mongodb://root:root@localhost:27017"
MONGO_DB = "forbes_2000"
MONGO_COLLECTION = "companies"

CLICKHOUSE_HOST = "http://localhost"
CLICKHOUSE_PORT = 8123  # HTTP port
CLICKHOUSE_DATABASE = "bronze"
CLICKHOUSE_TABLE = "companies_raw"

CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "etl")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "pass")


def _auth_tuple() -> Optional[Tuple[str, str]]:
	if CLICKHOUSE_USER or CLICKHOUSE_PASSWORD:
		return (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)
	return None


def fetch_companies_from_mongo() -> List[Dict[str, Any]]:
	client = MongoClient(MONGO_URI)
	db = client[MONGO_DB]
	collection = db[MONGO_COLLECTION]
	cursor = collection.find({}, {
		"_id": 0,
		"rank": 1,
		"company": 1,
		"ticker": 1,
		"headquarters": 1,
		"industry": 1,
		"sales_in_millions": 1,
		"profit_in_millions": 1,
		"assets_in_millions": 1,
		"market_value_in_millions": 1,
	})
	return list(cursor)


def ch_query(sql: str) -> None:
	url = f"{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
	params = {"query": sql}
	resp = requests.post(url, params=params, auth=_auth_tuple())
	resp.raise_for_status()


def truncate_table() -> None:
	"""Truncate the table before inserting (full refresh mode)"""
	sql = f"TRUNCATE TABLE IF EXISTS {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}"
	ch_query(sql)
	print(f"Truncated {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}")


def delete_by_ranks(ranks: List[int]) -> None:
	"""Delete specific ranks before re-inserting"""
	if not ranks:
		return
	ranks_str = ",".join(str(r) for r in ranks)
	sql = f"ALTER TABLE {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE} DELETE WHERE rank IN ({ranks_str})"
	ch_query(sql)
	print(f"Deleted {len(ranks)} existing records with matching ranks")


def insert_json_each_row(rows: List[Dict[str, Any]]) -> None:
	lines = []
	for r in rows:
		obj = {
			"rank": int(r.get("rank", 0) or 0),
			"company": str(r.get("company", "") or ""),
			"ticker": str(r.get("ticker", "") or ""),
			"headquarters": str(r.get("headquarters", "") or ""),
			"industry": str(r.get("industry", "") or ""),
			"sales_in_millions": float(r.get("sales_in_millions", 0) or 0),
			"profit_in_millions": float(r.get("profit_in_millions", 0) or 0),
			"assets_in_millions": float(r.get("assets_in_millions", 0) or 0),
			"market_value_in_millions": float(r.get("market_value_in_millions", 0) or 0),
		}
		lines.append(json.dumps(obj, ensure_ascii=False))

	if not lines:
		print("No rows to ingest.")
		return

	insert_sql = f"INSERT INTO {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE} FORMAT JSONEachRow"
	url = f"{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
	params = {
		"query": insert_sql,
		"database": CLICKHOUSE_DATABASE,
		"input_format_skip_unknown_fields": 1,
	}
	resp = requests.post(
		url,
		params=params,
		data="\n".join(lines).encode("utf-8"),
		headers={"Content-Type": "text/plain; charset=utf-8"},
		auth=_auth_tuple(),
	)
	resp.raise_for_status()
	print(f"Inserted {len(lines)} rows into ClickHouse {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}")


def upsert_into_clickhouse(rows: List[Dict[str, Any]]) -> None:
	if not rows:
		print("No rows to ingest.")
		return

	insert_json_each_row(rows)


def main() -> None:
	parser = argparse.ArgumentParser(
		description="Ingest company data from MongoDB to ClickHouse Bronze layer"
	)
	parser.add_argument(
		"--truncate",
		action="store_true",
		help="Truncate table before inserting (full refresh - deletes all existing data)"
	)
	parser.add_argument(
		"--force-deduplicate",
		action="store_true",
		help="Force immediate deduplication using OPTIMIZE TABLE (for ReplacingMergeTree)"
	)
	args = parser.parse_args()
	
	rows = fetch_companies_from_mongo()
	print(f"Fetched {len(rows)} documents from MongoDB")
	
	# Handle truncate option
	if args.truncate:
		truncate_table()
	
	# Insert data
	upsert_into_clickhouse(rows)
	
	# Force immediate deduplication if requested
	if args.force_deduplicate:
		print("Forcing deduplication...")
		optimize_sql = f"OPTIMIZE TABLE {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE} FINAL"
		ch_query(optimize_sql)
		print("Deduplication complete")
	else:
		print("\nNote: ReplacingMergeTree will deduplicate automatically during merges.")
		print("To force immediate deduplication, use --force-deduplicate flag.")


if __name__ == "__main__":
	main()


