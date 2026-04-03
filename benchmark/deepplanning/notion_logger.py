#!/usr/bin/env python3
"""
Notion Logger for DeepPlanning Experiment Results

Logs experiment results from aggregated JSON files to a Notion database.

Setup:
  1. Create an internal integration at https://www.notion.so/my-integrations
  2. Copy the API key into your .env file as NOTION_API_KEY=ntn_xxx
  3. Create a Notion database with the columns listed below
  4. Share the database with your integration (click "..." > "Connections" > add your integration)
  5. Copy the database ID from the URL:
     https://www.notion.so/<workspace>/<DATABASE_ID>?v=...
  6. Add NOTION_DATABASE_ID=<DATABASE_ID> to your .env file

Required Notion Database Columns:
  - Model (Title)
  - Date (Date)
  - Shopping Match Rate (Number, percent format)
  - Shopping Weighted Score (Number, percent format)
  - Travel Composite Score (Number, percent format)
  - Travel Case Acc (Number, percent format)
  - Travel Commonsense (Number, percent format)
  - Travel Personalized (Number, percent format)
  - Avg Acc (Number, percent format)
  - Domains (Multi-select)
  - Valid (Checkbox)
  - Notes (Rich text)

Usage:
  # Log from an aggregated results JSON file
  python notion_logger.py --file aggregated_results/gpt-4o-2024-11-20_aggregated.json

  # Log with a custom note
  python notion_logger.py --file aggregated_results/gpt-4o-2024-11-20_aggregated.json --notes "Baseline run, default params"

  # Log directly by specifying model name (runs aggregation first)
  python notion_logger.py --model gpt-4o-2024-11-20

  # List recent entries in the Notion database
  python notion_logger.py --list

  # Dry run (print what would be logged without sending to Notion)
  python notion_logger.py --file aggregated_results/gpt-4o-2024-11-20_aggregated.json --dry-run
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from dotenv import load_dotenv
from notion_client import Client


def get_notion_client() -> Client:
    """Initialize Notion client from environment variables."""
    load_dotenv(Path(__file__).parent / ".env")
    api_key = os.getenv("NOTION_API_KEY")
    if not api_key:
        print("Error: NOTION_API_KEY not found in .env file")
        print("Add NOTION_API_KEY=ntn_xxx to your .env file")
        sys.exit(1)
    return Client(auth=api_key)


def get_database_id() -> str:
    """Get Notion database ID from environment variables."""
    load_dotenv(Path(__file__).parent / ".env")
    db_id = os.getenv("NOTION_DATABASE_ID")
    if not db_id:
        print("Error: NOTION_DATABASE_ID not found in .env file")
        print("Add NOTION_DATABASE_ID=<your-database-id> to your .env file")
        sys.exit(1)
    return db_id


def load_aggregated_results(file_path: Path) -> Dict[str, Any]:
    """Load aggregated results from a JSON file."""
    if not file_path.exists():
        print(f"Error: File not found: {file_path}")
        sys.exit(1)
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_notion_properties(data: Dict[str, Any], notes: str = "") -> Dict[str, Any]:
    """Build Notion page properties from aggregated results."""
    overall = data.get("overall", {})
    domains = data.get("domains", {})
    shopping = domains.get("shopping", {})
    travel = domains.get("travel", {})

    properties = {
        "Model": {
            "title": [{"text": {"content": data.get("model_name", "unknown")}}]
        },
        "Date": {
            "date": {"start": datetime.now().strftime("%Y-%m-%d")}
        },
        "Domains": {
            "multi_select": [
                {"name": d} for d in overall.get("domains_completed", [])
            ]
        },
        "Valid": {
            "checkbox": overall.get("valid", False)
        },
    }

    # Shopping metrics
    if shopping:
        properties["Shopping Match Rate"] = {
            "number": round(shopping.get("match_rate", 0.0), 4)
        }
        properties["Shopping Weighted Score"] = {
            "number": round(shopping.get("weighted_average_case_score", 0.0), 4)
        }

    # Travel metrics
    if travel:
        properties["Travel Composite Score"] = {
            "number": round(travel.get("composite_score", 0.0), 4)
        }
        properties["Travel Case Acc"] = {
            "number": round(travel.get("case_acc", 0.0), 4)
        }
        properties["Travel Commonsense"] = {
            "number": round(travel.get("commonsense_score", 0.0), 4)
        }
        properties["Travel Personalized"] = {
            "number": round(travel.get("personalized_score", 0.0), 4)
        }

    # Cross-domain average
    if "avg_acc" in overall:
        properties["Avg Acc"] = {
            "number": round(overall["avg_acc"], 4)
        }

    # Notes
    if notes:
        properties["Notes"] = {
            "rich_text": [{"text": {"content": notes}}]
        }

    return properties


def log_to_notion(data: Dict[str, Any], notes: str = "", dry_run: bool = False):
    """Log experiment results to Notion database."""
    properties = build_notion_properties(data, notes)

    if dry_run:
        print("\n=== DRY RUN - Would log the following to Notion ===")
        print(json.dumps(properties, indent=2))
        return

    notion = get_notion_client()
    db_id = get_database_id()

    response = notion.pages.create(
        parent={"database_id": db_id},
        properties=properties,
    )

    print(f"Logged to Notion: {data.get('model_name', 'unknown')}")
    print(f"Page URL: {response.get('url', 'N/A')}")


def list_recent_entries(limit: int = 10):
    """List recent entries in the Notion database."""
    notion = get_notion_client()
    db_id = get_database_id()

    response = notion.databases.query(
        database_id=db_id,
        sorts=[{"property": "Date", "direction": "descending"}],
        page_size=limit,
    )

    if not response["results"]:
        print("No entries found.")
        return

    print(f"\n{'Model':<35} {'Date':<12} {'Avg Acc':<10} {'Domains'}")
    print("-" * 80)

    for page in response["results"]:
        props = page["properties"]

        # Extract model name
        title = props.get("Model", {}).get("title", [])
        model = title[0]["text"]["content"] if title else "?"

        # Extract date
        date_prop = props.get("Date", {}).get("date")
        date = date_prop["start"] if date_prop else "?"

        # Extract avg_acc
        avg_acc = props.get("Avg Acc", {}).get("number")
        avg_acc_str = f"{avg_acc:.4f}" if avg_acc is not None else "N/A"

        # Extract domains
        domains = props.get("Domains", {}).get("multi_select", [])
        domains_str = ", ".join(d["name"] for d in domains)

        print(f"{model:<35} {date:<12} {avg_acc_str:<10} {domains_str}")


def main():
    parser = argparse.ArgumentParser(
        description="Log DeepPlanning experiment results to Notion"
    )
    parser.add_argument(
        "--file", type=str, help="Path to aggregated results JSON file"
    )
    parser.add_argument(
        "--model",
        type=str,
        help="Model name (will look for aggregated_results/<model>_aggregated.json)",
    )
    parser.add_argument("--notes", type=str, default="", help="Optional notes")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be logged without sending to Notion",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List recent entries in the Notion database",
    )

    args = parser.parse_args()

    if args.list:
        list_recent_entries()
        return

    # Determine the file to load
    if args.file:
        file_path = Path(args.file)
    elif args.model:
        project_root = Path(__file__).resolve().parent
        file_path = project_root / "aggregated_results" / f"{args.model}_aggregated.json"
    else:
        parser.print_help()
        sys.exit(1)

    data = load_aggregated_results(file_path)

    print(f"\nModel: {data.get('model_name', 'unknown')}")
    overall = data.get("overall", {})
    if "avg_acc" in overall:
        print(f"Avg Acc: {overall['avg_acc']:.4f} ({overall['avg_acc']:.2%})")
    print(f"Domains: {', '.join(overall.get('domains_completed', []))}")

    log_to_notion(data, notes=args.notes, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
