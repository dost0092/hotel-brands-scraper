
# Hotel Scrapers (Hilton · IHG · Marriott · Hyatt)

A production-ready Selenium scraping framework for collecting hotel data from multiple brands, Hilton, IHG, Marriott, and Hyatt with support for parallel execution, auto-restart, and Python 3.12 compatibility.

```

hotel_brands_scraper/
│
├── main.py
│   # Main runner script
│   # - Can execute scrapers individually or in parallel
│
├── requirements.txt
│   # Python dependencies
│
├── README.md
│   # Project documentation
│
├── config/
│   ├── HiltonConfig.py
│   ├── ihgconfig.py
│   ├── MarriottConfig.py
│   └── Hyattconfig.py
│   # Brand-specific configuration files
│   # (URLs, selectors, filters, timeouts, etc.)
│
├── scrapers/
│   ├── __init__.py
│   ├── hilton.py
│   ├── ihg.py
│   ├── marriott.py
│   └── hyatt.py
│   # Brand-specific scraper implementations
│
├── csv/
│   ├── hilton.csv
│   ├── ihg.csv
│   ├── marriott.csv
│   └── hyatt.csv
│   # Final scraped data in CSV format
│
└── json/
    ├── hilton.json
    ├── ihg.json
    ├── marriott.json
    └── hyatt.json
    # Final scraped data in JSON format
```

# How It Works

Each hotel brand has:

- A dedicated **config file** (`config/`) defining URLs, selectors, and behavior
- A dedicated **scraper module** (`scrapers/`) handling extraction logic

The `main.py` script:

- Runs one or multiple scrapers
- Handles retries, restarts, and orchestration

Output is saved in both:

- **CSV format** (`csv/`)
- **JSON format** (`json/`)

---

#  Installation

```bash
pip install -r requirements.txt


```
▶️ Usage
Run all scrapers:
```
python main.py


```
Or run individual scrapers (if supported by main.py logic):
```
python main.py hilton
python main.py ihg

```
Output

CSV files for easy analysis and spreadsheets

JSON files for APIs, databases, or further processing





Video Reference Link:
https://www.loom.com/share/3e784f180c0c462fbed16aebf7bf95fd