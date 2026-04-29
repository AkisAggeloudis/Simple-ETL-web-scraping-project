import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3

# URL and attributes
url = "https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ['Bank_Name', 'MC_USD_Billion']


# ---------------- LOGGING FUNCTION ----------------
def log_progress(message):
    timestamp_format = '%Y-%b-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)

    with open("./etl_project_log.txt", "a") as f:
        f.write(timestamp + " : " + message + "\n")


# ---------------- EXTRACT FUNCTION ----------------
def extract(url, table_attribs):
    page = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=10
    ).text

    soup = BeautifulSoup(page, 'html.parser')

    df = pd.DataFrame(columns=table_attribs)

    tables = soup.find_all('table')
    target_table = tables[2]

    rows = target_table.find_all('tr')

    for row in rows:
        cols = row.find_all('td')

        if len(cols) >= 3:
            bank_name = cols[0].get_text(strip=True)
            market_cap = cols[2].get_text(strip=True)

            if market_cap and market_cap != '—':
                data_dict = {
                    "Bank_Name": bank_name,
                    "MC_USD_Billion": market_cap
                }

                df = pd.concat(
                    [df, pd.DataFrame([data_dict])],
                    ignore_index=True
                )

    return df


# ---------------- TRANSFORM FUNCTION ----------------
def transform(df, csv_path):
    exchange_df = pd.read_csv(csv_path)

    exchange_rate = dict(
        zip(exchange_df['Currency'], exchange_df['Rate'])
    )

    df['MC_USD_Billion'] = (
        df['MC_USD_Billion']
        .str.replace(',', '')
        .str.strip()
        .astype(float)
    )

    gbp_rate = float(exchange_rate['GBP'])
    eur_rate = float(exchange_rate['EUR'])
    inr_rate = float(exchange_rate['INR'])

    df['MC_GBP_Billion'] = [
        np.round(x * gbp_rate, 2) for x in df['MC_USD_Billion']
    ]

    df['MC_EUR_Billion'] = [
        np.round(x * eur_rate, 2) for x in df['MC_USD_Billion']
    ]

    df['MC_INR_Billion'] = [
        np.round(x * inr_rate, 2) for x in df['MC_USD_Billion']
    ]

    return df


# ---------------- LOAD TO CSV ----------------
def load_to_csv(df, output_path):
    df.to_csv(output_path, index=False)


# ---------------- LOAD TO DATABASE ----------------
def load_to_db(df, sql_connection, table_name):
    df.to_sql(
        table_name,
        sql_connection,
        if_exists='replace',
        index=False
    )


# ---------------- RUN SQL QUERY ----------------
def run_query(query_statement, sql_connection):
    print("\nQUERY:", query_statement)

    result = pd.read_sql(query_statement, sql_connection)
    print(result)


# ---------------- MAIN PIPELINE ----------------
if __name__ == "__main__":

    log_progress("Preliminaries complete. Starting ETL process")

    # Extract
    df = extract(url, table_attribs)
    log_progress("Data extraction complete")

    # Transform
    df = transform(df, "exchange_rate.csv")
    log_progress("Data transformation complete")

    print("5th largest bank Market Cap in EUR:", df['MC_EUR_Billion'][4])

    # Load to CSV
    load_to_csv(df, "Largest_banks_data.csv")
    log_progress("Data saved to CSV")

    # Load to DB
    conn = sqlite3.connect("Banks.db")
    load_to_db(df, conn, "Largest_banks")
    log_progress("Data loaded into database")

    # Queries
    run_query("SELECT * FROM Largest_banks", conn)
    run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", conn)
    run_query("SELECT Bank_Name FROM Largest_banks LIMIT 5", conn)

    log_progress("ETL process completed successfully")