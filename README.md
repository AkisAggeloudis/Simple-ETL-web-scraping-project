# Largest Banks ETL Project

This project is a simple **ETL (Extract, Transform, Load)** pipeline built with Python. It collects data about the **largest banks in the world by market capitalization** from Wikipedia, transforms the values into multiple currencies, stores the results in a CSV file, and loads the data into a SQLite database for querying.

---

## Features

- Extracts bank market capitalization data from Wikipedia
- Cleans and transforms raw scraped data
- Converts USD values into:
  - GBP (£)
  - EUR (€)
  - INR (₹)
- Saves processed data to CSV
- Loads data into SQLite database
- Runs SQL queries for analysis
- Logs ETL progress with timestamps

---

## Technologies Used

- Python 3
- Pandas
- NumPy
- Requests
- BeautifulSoup4
- lxml
- SQLite3

---

## Project Structure

```bash
├── etl_project.py
├── exchange_rate.csv
├── Largest_banks_data.csv
├── Banks.db
├── etl_project_log.txt
└── README.md
 Installation

Clone the repository:

git clone https://github.com/yourusername/largest-banks-etl.git
cd largest-banks-etl

Install required dependencies:

pip install pandas numpy requests beautifulsoup4 lxml
How It Works
1)Extract

The script scrapes data from:

https://en.wikipedia.org/wiki/List_of_largest_banks

It extracts:

Bank_Name
MC_USD_Billion
2)Transform

Using exchange rates stored in:

exchange_rate.csv

It converts market capitalization from USD into:

MC_GBP_Billion
MC_EUR_Billion
MC_INR_Billion
3)Load

The transformed dataset is:

Exported as Largest_banks_data.csv
Loaded into SQLite database Banks.db
4)Query

The script runs sample SQL queries:

SELECT * FROM Largest_banks;
SELECT AVG(MC_GBP_Billion) FROM Largest_banks;
SELECT Bank_Name FROM Largest_banks LIMIT 5;
- Run the Project
python etl_project.py
- Example Output
5th largest bank Market Cap in EUR: 94.72

QUERY: SELECT AVG(MC_GBP_Billion) FROM Largest_banks
- Logging

Progress is saved to:

etl_project_log.txt

Example:

2026-Apr-29-12:30:11 : Data extraction complete.
2026-Apr-29-12:30:14 : Data transformation complete.
- Sample exchange_rate.csv Format
Currency,Rate
GBP,0.78
EUR,0.92
INR,83.10
- Future Improvements
Add automated scheduling with Cron / Task Scheduler
Store historical bank rankings
Add data visualizations
Use APIs instead of web scraping
Dockerize project
