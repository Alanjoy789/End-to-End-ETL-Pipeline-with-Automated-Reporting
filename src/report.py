"""
Report Module
=============
Generates a self-contained HTML dashboard from the latest weather data.
Uses Jinja2 templates and Chart.js for interactive charts.
"""

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from jinja2 import Environment, FileSystemLoader

from src.load import get_latest_data, get_pipeline_history

logger = logging.getLogger(__name__)


def prepare_report_data(config: dict) -> dict:
    """
    Prepare all data needed for the dashboard template.

    Returns
    -------
    dict
        Template context with weather data, stats, and chart data.
    """
    df = get_latest_data(config)
    history = get_pipeline_history(config, limit=5)

    if df.empty:
        logger.warning("No data available for reporting")
        return {"has_data": False, "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M UTC")}

    # Summary statistics
    stats = {
        "total_cities": len(df),
        "avg_temp": round(df["temperature_c"].mean(), 1),
        "max_temp_city": df.loc[df["temperature_c"].idxmax(), "city"],
        "max_temp": round(df["temperature_c"].max(), 1),
        "min_temp_city": df.loc[df["temperature_c"].idxmin(), "city"],
        "min_temp": round(df["temperature_c"].min(), 1),
        "avg_humidity": round(df["humidity_pct"].mean(), 1),
        "avg_wind": round(df["wind_speed_mps"].mean(), 1),
        "most_comfortable_city": df.loc[df["comfort_index"].idxmax(), "city"] if "comfort_index" in df.columns else "N/A",
        "best_comfort_score": round(df["comfort_index"].max(), 1) if "comfort_index" in df.columns else 0,
    }

    # Chart data
    cities = [str(x) for x in df["city"]]
    temperatures = [float(x) for x in df["temperature_c"]]
    feels_like = [float(x) for x in df["feels_like_c"]]
    humidity = [float(x) for x in df["humidity_pct"]]
    wind_speed = [float(x) for x in df["wind_speed_mps"]]
    comfort = [float(x) for x in df["comfort_index"]] if "comfort_index" in df.columns else []

    # Weather conditions for pie chart
    condition_counts = df["weather_condition"].value_counts()
    conditions = [str(x) for x in condition_counts.index]
    condition_values = [int(x) for x in condition_counts.values]

    # City detail cards
    city_cards = []
    for _, row in df.iterrows():
        city_cards.append({
            "city": row["city"],
            "country": row.get("country", ""),
            "temp": round(row["temperature_c"], 1),
            "feels_like": round(row["feels_like_c"], 1) if pd.notna(row.get("feels_like_c")) else "N/A",
            "humidity": round(row["humidity_pct"], 1),
            "wind": round(row["wind_speed_mps"], 1),
            "condition": row.get("weather_condition", "Unknown"),
            "description": row.get("weather_description", "N/A"),
            "icon": row.get("weather_icon", "01d"),
            "temp_category": str(row.get("temperature_category", "N/A")),
            "comfort": round(row["comfort_index"], 1) if pd.notna(row.get("comfort_index")) else 0,
            "pressure": round(row.get("pressure_hpa", 0), 1),
            "visibility": round(row.get("visibility_m", 0) / 1000, 1) if pd.notna(row.get("visibility_m")) else "N/A",
        })

    # Pipeline run history
    run_history = []
    for _, row in history.iterrows():
        run_history.append({
            "run_id": row["run_id"],
            "start": row["run_start_utc"][:19],
            "status": row["status"],
            "records": row["records_loaded"],
            "duration": row["duration_seconds"],
        })

    return {
        "has_data": True,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M UTC"),
        "stats": stats,
        "cities": cities,
        "temperatures": temperatures,
        "feels_like": feels_like,
        "humidity": humidity,
        "wind_speed": wind_speed,
        "comfort": comfort,
        "conditions": conditions,
        "condition_values": condition_values,
        "city_cards": city_cards,
        "run_history": run_history,
    }


def generate_report(config: dict) -> str:
    """
    Generate the HTML dashboard report.

    Returns
    -------
    str
        Path to the generated HTML file.
    """
    template_dir = Path(config["reporting"]["template"]).parent
    template_name = Path(config["reporting"]["template"]).name
    output_path = config["reporting"]["output"]

    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    env = Environment(loader=FileSystemLoader(str(template_dir)))
    template = env.get_template(template_name)

    context = prepare_report_data(config)
    html = template.render(**context)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    logger.info(f"Report generated: {output_path}")
    return output_path