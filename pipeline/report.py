"""
Report generation.
Queries pipeline results and generates a markdown summary.
Includes an optional LLM enhancement stub.
"""

from pathlib import Path
from datetime import datetime

import psycopg
import pandas as pd
import os 

def collect_metrics(conn: psycopg.Connection, quarantine_dir: str) -> dict:
    """Collect all pipeline metrics into a single dictionary."""

    metrics = {}

    with conn.cursor() as cur:
        cur.execute("SELECT * FROM v_daily_metrics ORDER BY order_date;")
        metrics["daily"] = cur.fetchall()

    with conn.cursor() as cur:
        cur.execute("SELECT * FROM v_top_customers_by_spend;")
        metrics["top_customers"] = cur.fetchall()

    with conn.cursor() as cur:
        cur.execute("SELECT * FROM v_top_skus;")
        metrics["top_skus"] = cur.fetchall()

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM customers;")
        metrics["customer_count"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM orders;")
        metrics["order_count"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM order_items;")
        metrics["item_count"] = cur.fetchone()[0]

    metrics["quarantine"] = {}
    quarantine_path = Path(quarantine_dir)
    for filename in ["customers.csv", "orders.csv", "order_items.csv"]:
        filepath = quarantine_path / filename
        if filepath.exists():
            qdf = pd.read_csv(filepath)
            table_name = filename.replace(".csv", "")
            metrics["quarantine"][table_name] = {
                "count": len(qdf),
                "reasons": qdf["_rejection_reason"].value_counts().to_dict()
                if "_rejection_reason" in qdf.columns else {}
            }

    return metrics


def build_prompt(metrics: dict) -> str:
    """Build a concise prompt for the LLM."""

    prompt = """You are a data analyst. Write a 3-paragraph executive summary for this ETL pipeline run.

LOADED: {c} customers, {o} orders, {i} items

QUARANTINED:
""".format(
        c=metrics['customer_count'],
        o=metrics['order_count'],
        i=metrics['item_count'],
    )

    for table, info in metrics["quarantine"].items():
        prompt += f"  {table}: {info['count']} rejected\n"
        for reason, count in info["reasons"].items():
            prompt += f"    - {reason}: {count}\n"

    prompt += "\nHighlight data quality health, concerns, and recommended actions."

    return prompt


def call_gemini(prompt: str, llm_config: dict) -> str:
    """
    Call Google Gemini API for executive summary.
    Falls back gracefully if unavailable.
    """

    try:
        import httpx

        api_key = os.getenv(llm_config.get("api_key_env", "GEMINI_API_KEY"))
        if not api_key:
            return "*GEMINI_API_KEY not set in .env. Add your key to enable AI summaries.*"

        model = llm_config.get("model", "gemini-2.0-flash")
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"

        payload = {
            "contents": [
                {
                    "parts": [
                        {"text": prompt}
                    ]
                }
            ],
            "generationConfig": {
                "temperature": 0.3,
                "maxOutputTokens": 1024,
            }
        }

        response = httpx.post(url, json=payload, timeout=30.0)

        if response.status_code == 200:
            data = response.json()
            return data["candidates"][0]["content"]["parts"][0]["text"]
        else:
            return f"*Gemini API returned status {response.status_code}: {response.text}*"

    except ImportError:
        return "*httpx not installed. Run `pip install httpx` for LLM support.*"
    except Exception as e:
        return f"*Gemini unavailable ({e}). See LLM Integration Guide below.*"


def generate_report(
    conn: psycopg.Connection,
    quarantine_dir: str,
    output_path: str = "REPORT.md",
    llm_config: dict = None,
) -> None:
    """Generate a markdown report with optional LLM executive summary."""

    sections = []

    sections.append("# Pipeline Report")
    sections.append(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")

    # Collect metrics
    metrics = collect_metrics(conn, quarantine_dir)

    # --- LLM Executive Summary ---
    if llm_config and llm_config.get("enabled"):
        sections.append("## Executive Summary (AI-Generated)\n")
        prompt = build_prompt(metrics)
        summary = call_gemini(prompt, llm_config)
        sections.append(summary)
        sections.append("")
    else:
        sections.append("## Executive Summary\n")
        sections.append("*LLM not enabled. Set `llm.enabled: true` in config.yaml to generate AI-powered summaries.*\n")

    # --- Section 1: Daily Metrics ---
    sections.append("## Daily Order Metrics\n")
    sections.append("| Date | Orders | Revenue | Avg Order Value |")
    sections.append("|------|--------|---------|-----------------|")
    for row in metrics["daily"]:
        sections.append(f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} |")

    # --- Section 2: Top Customers ---
    sections.append("\n## Top Customers by Lifetime Spend\n")
    sections.append("| Rank | Customer | Email | Orders | Spend |")
    sections.append("|------|----------|-------|--------|-------|")
    for row in metrics["top_customers"]:
        sections.append(f"| {row[0]} | {row[2]} | {row[3]} | {row[4]} | {row[5]} |")

    # --- Section 3: Top SKUs ---
    sections.append("\n## Top SKUs by Revenue\n")
    sections.append("| Rank | SKU | Category | Units Sold | Revenue |")
    sections.append("|------|-----|----------|------------|---------|")
    for row in metrics["top_skus"]:
        sections.append(f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} |")

    # --- Section 4: Data Quality Summary ---
    sections.append("\n## Data Quality Findings\n")
    for table, info in metrics["quarantine"].items():
        sections.append(f"### Quarantined: {table}\n")
        sections.append(f"**{info['count']} rows rejected:**\n")
        for reason, count in info["reasons"].items():
            sections.append(f"- `{reason}`: {count} row(s)")
        sections.append("")

    # --- Section 5: LLM Integration Guide ---
    sections.append("## LLM Integration Guide\n")
    sections.append("This report supports AI-powered executive summaries Using Gemini.\n")
    sections.append("### Google Gemini")
    sections.append("```bash")
    sections.append("# Get a free API key at https://aistudio.google.com")
    sections.append("```")
    sections.append("```yaml")
    sections.append("# config.yaml")
    sections.append("llm:")
    sections.append("  enabled: true")
    sections.append("  provider: gemini")
    sections.append("  model: gemini-2.5-flash")
    sections.append("  api_key_env: GEMINI_API_KEY")
    sections.append("```")
    sections.append("```env")
    sections.append("# .env")
    sections.append("GEMINI_API_KEY=your-key-here")
    sections.append("```\n")

    # Write report
    report_content = "\n".join(sections)
    Path(output_path).write_text(report_content, encoding="utf-8")