# 🔄 End-to-End ETL Pipeline with Automated Reporting

An end-to-end **Extract, Transform, Load (ETL)** pipeline that pulls real-time weather data from the [OpenWeatherMap API](https://openweathermap.org/api), transforms and enriches it, loads it into a **SQLite** database, and generates an automated **HTML dashboard** with key weather insights.

Built as a portfolio project demonstrating **Data Engineering** and **Data Analytics** skills.

![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python)
![SQLite](https://img.shields.io/badge/Database-SQLite-green?logo=sqlite)
![License](https://img.shields.io/badge/License-MIT-yellow)
![Status](https://img.shields.io/badge/Status-Complete-brightgreen)

---

## 📐 Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  EXTRACT     │────▶│  TRANSFORM   │────▶│  LOAD        │────▶│  REPORT      │
│              │     │              │     │              │     │              │
│ OpenWeather  │     │ Clean data   │     │ SQLite DB    │     │ HTML         │
│ API (5 cities│     │ Enrich       │     │ Upsert rows  │     │ Dashboard    │
│ configurable)│     │ Validate     │     │ Log runs     │     │ + Charts     │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
```

## 🚀 Features

- **Configurable city list** — track weather for any cities worldwide
- **Robust extraction** with retry logic and error handling
- **Data validation** — schema checks, null handling, range validation
- **Idempotent loads** — upsert logic prevents duplicate records
- **Pipeline logging** — every run is tracked with status, row counts, and duration
- **Automated HTML dashboard** — generates a self-contained report with charts
- **Scheduler-ready** — includes cron/Task Scheduler setup instructions
- **Fully tested** — unit tests for each pipeline stage

---

## 🛠️ Tech Stack

| Layer       | Technology                     |
|-------------|-------------------------------|
| Language    | Python 3.9+                   |
| Extraction  | `requests` (REST API)         |
| Transform   | `pandas`                      |
| Database    | SQLite (via `sqlite3`)        |
| Reporting   | `Jinja2` + `Chart.js` (HTML)  |
| Testing     | `pytest`                      |
| Scheduling  | `cron` (Linux) / Task Scheduler (Windows) |

---

## 📁 Project Structure

```
etl-pipeline-project/
├── config/
│   └── pipeline_config.yaml     # Cities, API settings, DB path
├── src/
│   ├── __init__.py
│   ├── extract.py               # API data extraction
│   ├── transform.py             # Cleaning & enrichment
│   ├── load.py                  # Database loading
│   ├── report.py                # Dashboard generation
│   └── pipeline.py              # Orchestrator (runs full ETL)
├── dashboards/
│   └── template.html            # Jinja2 report template
├── tests/
│   ├── __init__.py
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── data/
│   ├── raw/                     # Raw API responses (JSON)
│   └── processed/               # Cleaned CSVs
├── logs/                        # Pipeline run logs
├── requirements.txt
├── setup.py
├── .gitignore
├── LICENSE
└── README.md
```

---

## ⚡ Quick Start

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/etl-pipeline-project.git
cd etl-pipeline-project
```

### 2. Create a virtual environment
```bash
python -m venv venv
source venv/bin/activate        # Linux/Mac
# venv\Scripts\activate         # Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Get your API key
- Sign up at [OpenWeatherMap](https://openweathermap.org/api) (free tier)
- Copy your API key

### 5. Configure the pipeline
Edit `config/pipeline_config.yaml`:
```yaml
api:
  key: "YOUR_API_KEY_HERE"   # Paste your key
  cities:                     # Add/remove cities
    - London
    - New York
    - Tokyo
    - Mumbai
    - Sydney
```

### 6. Run the pipeline
```bash
python -m src.pipeline
```

### 7. View the dashboard
Open `dashboards/weather_report.html` in your browser.

---

## ⏰ Scheduling (Automated Runs)

### Linux/Mac (cron) — Run every 3 hours
```bash
crontab -e
# Add this line:
0 */3 * * * cd /path/to/etl-pipeline-project && /path/to/venv/bin/python -m src.pipeline >> logs/cron.log 2>&1
```

### Windows (Task Scheduler)
1. Open Task Scheduler → Create Basic Task
2. Set trigger to repeat every 3 hours
3. Action: Start a Program → `python.exe` with arguments `-m src.pipeline`
4. Set "Start in" to the project directory

---

## 📊 Sample Dashboard Output

The generated dashboard includes:
- **Temperature comparison** across all tracked cities (bar chart)
- **Humidity levels** with color-coded indicators
- **Wind speed analysis**
- **Weather condition breakdown**
- **Pipeline run metadata** (timestamp, records processed, status)

---

## 🧪 Running Tests

```bash
pytest tests/ -v
```

---

## 🔮 Future Enhancements

- [ ] Migrate to PostgreSQL for production use
- [ ] Add Apache Airflow DAG for orchestration
- [ ] Implement data quality checks with Great Expectations
- [ ] Add historical trend analysis (7-day / 30-day)
- [ ] Deploy dashboard to GitHub Pages
- [ ] Add Slack/email alerts on pipeline failure

---

## 📝 License

This project is licensed under the MIT License — see [LICENSE](LICENSE) for details.

---

## 🤝 Contact

Built by **[Your Name]** — aspiring Data Engineer & Analyst.

[LinkedIn](https://linkedin.com/in/YOUR_PROFILE) · [Portfolio](https://your-portfolio.com)
