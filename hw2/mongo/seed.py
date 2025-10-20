from configparser import ConfigParser
from pymongo import MongoClient
from pandas import DataFrame
from pprint import pprint
from typing import List
from tqdm import tqdm
import yfinance as yf
import pandas as pd
import os

config = ConfigParser()
config_path = os.path.join(
    os.path.dirname(__file__),
    "data.ini"
)
config.read(config_path)

FORBES_2000_CSV = os.path.join(os.path.dirname(__file__), "./data/forbes_2000_companies_2025.csv")
TICKER_CSV = os.path.join(os.path.dirname(__file__), "./data/ticker_symbols.csv")
TICKER_NOT_FOUND_CSV = os.path.join(os.path.dirname(__file__), "./data/ticker_not_found.csv")
MONGO_UPLOAD_FAILED_CSV = os.path.join(os.path.dirname(__file__), "./data/mongo_upload_failed.csv")


def fix_amount(value: str) -> float:
	if isinstance(value, (int, float)):
		return float(value) * 1000

	if value is None or value.strip() == "":
		return 0.0

	return float(value.replace(",", "").replace("$", "").strip()) * 1000

print("\nStarting MongoDB seeding script\n")
print("Connecting to database")

client: MongoClient = MongoClient(
	config.get("mongo", "url"),
	username=config.get("mongo", "username"),
	password=config.get("mongo", "password")
)
print(f"Connection: {client.admin.command("ping")}")

db: MongoClient = client[db_name := config.get("mongo", "database")]
collection: MongoClient = db[config.get("mongo", "collection")]
if collection.name not in db.list_collection_names():
	raise Exception(f"Collection '{collection.name}' does not exist.")

# Data loading
if not os.path.exists(FORBES_2000_CSV):
	raise Exception(f"CSV file '{FORBES_2000_CSV}' not found.")

df: DataFrame = pd.read_csv(FORBES_2000_CSV, delimiter=";")

if os.path.exists(TICKER_CSV):
	ticker_df: DataFrame = pd.read_csv(TICKER_CSV, delimiter=";")
else:
	ticker_df = pd.DataFrame(columns=["Rank", "Company", "Ticker"])
	ticker_not_found_df = pd.DataFrame(columns=["Rank", "Company"])

	for _, row in tqdm(df.iterrows(), total=len(df), desc="Searching tickers from yfinance"):
		row = row.to_dict()
		rank: str = row["Rank"]
		comany: str = row["Company"]
		symbol: str = None

		#pprint(f"Looking up ticker for Company: {comany}, Rank: {rank}")

		try:
			results: DataFrame = yf.Lookup(comany).get_stock(count=1)
			symbol = results.index[0]
		except Exception:
			try:
				results: DataFrame = yf.Search(comany, max_results=1).quotes
				symbol = results[0].get("symbol")
			except Exception:
				try:
					results: DataFrame = yf.Search(comany, max_results=1, include_research=True).all

					related_tickers: List[any] = []
					for news_item in results.get('news', []):
						tickers = news_item.get('relatedTickers')
						if tickers:
							related_tickers.extend(tickers)

					related_tickers = list(set(related_tickers))

					if not related_tickers or len(related_tickers) == 0:
						raise Exception("No related tickers found.")

					symbol = related_tickers[0]

				except Exception:
					#pprint(f"Could not find ticker for Company: {comany}, Rank: {rank}")
					ticker_not_found_df.loc[len(ticker_not_found_df)] = {
						"Rank": rank,
						"Company": comany
					}
					continue


		#pprint(f"Found ticker: {symbol} for Company: {comany}")
		ticker_df.loc[len(ticker_df)] = {
			"Rank": rank,
			"Company": comany,
			"Ticker": symbol
		}

	ticker_df.to_csv(TICKER_CSV, sep=";", index=False)
	ticker_not_found_df.to_csv(TICKER_NOT_FOUND_CSV, sep=";", index=False)

df = pd.merge(df, ticker_df, on=["Rank", "Company"], how="left")
mongo_upload_failed_df = pd.DataFrame(columns=(list(df.columns) + ["Reason"]))

for _, row in tqdm(df.iterrows(), total=len(df), desc="Inserting companies into MongoDB"):
	row = row.to_dict()

	row = {
		key: value.strip() if isinstance(value, str) else value
		for key, value in row.items()
	}

	# Rank;Company;Headquarters;Industry;Sales ($B);Profit ($B);Assets ($B);Market Value ($B)
	# + Ticker
	try:
		document = {
			"rank": 					int(row["Rank"]),
			"company": 					row["Company"],
			"ticker": 					row["Ticker"],
			"headquarters": 			row["Headquarters"],
			"industry": 				row["Industry"],
			"sales_in_millions": 		fix_amount(row["Sales ($B)"]),
			"profit_in_millions":  		fix_amount(row["Profit ($B)"]),
			"assets_in_millions":  		fix_amount(row["Assets ($B)"]),
			"market_value_in_millions": fix_amount(row["Market Value ($B)"]),

		}
	except ValueError as e:
		#pprint(f"Error processing row for Company: {row["Company"]}, Rank: {row["Rank"]} - {e}")
		mongo_upload_failed_df.loc[len(mongo_upload_failed_df)] = {
			**row,
			"Reason": str(e)
		}
		continue

	#pprint(f"Inserting document for Company: {row["Company"]}, Rank: {row["Rank"]}")

	if collection.find_one({"rank": document["rank"]}):
		collection.replace_one({"rank": document["rank"]}, document)
	else:
		collection.insert_one(document)

mongo_upload_failed_df.to_csv(MONGO_UPLOAD_FAILED_CSV, sep=";", index=False)

print("\nMongoDB seeding script completed successfully\n")