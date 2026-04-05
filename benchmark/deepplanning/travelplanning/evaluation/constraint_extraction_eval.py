"""
Evaluate constraint extraction accuracy by comparing LLM-extracted constraints
against ground-truth hard_constraints from the query metadata.

Usage:
    python -m evaluation.constraint_extraction_eval \
        --model gpt-5.4 \
        --language en \
        --samples 10
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from agent.constraint_extractor import (
    extract_constraints,
    ConstraintTuple,
    TripMeta,
    constraints_to_dicts,
)


# Ground-truth constraint type → expected variable prefix mapping
GT_TO_VARIABLE_MAP = {
    "train_": "transport",
    "flight_": "transport",
    "hotel_": "hotel",
    "restaurant_": "restaurant",
    "attraction_": "attraction",
    "budget_": "budget",
}


def _ground_truth_entities(hard_constraints: Dict[str, Any]) -> Dict[str, set]:
    """
    Extract the key entity names/values from ground-truth constraints.

    Returns dict with keys: transport, hotel, restaurant, attraction, budget
    Each maps to a set of identifying values the plan must contain.
    """
    entities: Dict[str, set] = {
        "transport": set(),
        "hotel": set(),
        "restaurant": set(),
        "attraction": set(),
        "budget": set(),
    }

    for cname, cdata in hard_constraints.items():
        if cname.startswith("train_") or cname.startswith("flight_"):
            for key in ("outbound_train_no", "inbound_train_no",
                        "outbound_flight_no", "inbound_flight_no"):
                if key in cdata:
                    entities["transport"].add(cdata[key])
        elif cname.startswith("hotel_"):
            if "hotel_name" in cdata:
                entities["hotel"].add(cdata["hotel_name"])
            if "hotel_star" in cdata:
                entities["hotel"].add(f"star={cdata['hotel_star']}")
            if "required_service" in cdata:
                entities["hotel"].add(f"service={cdata['required_service']}")
        elif cname.startswith("restaurant_"):
            if "restaurant_name" in cdata:
                entities["restaurant"].add(cdata["restaurant_name"])
            if "required_tag" in cdata:
                entities["restaurant"].add(f"tag={cdata['required_tag']}")
        elif cname.startswith("attraction_"):
            for name in cdata.get("attraction_names", []):
                entities["attraction"].add(name)
        elif cname == "budget_constraint":
            entities["budget"].add(f"max={cdata.get('max_budget', '?')}")

    return entities


def _extracted_entities(constraints: List[ConstraintTuple]) -> Dict[str, set]:
    """
    Extract the key entity names/values from LLM-extracted constraints.
    """
    entities: Dict[str, set] = {
        "transport": set(),
        "hotel": set(),
        "restaurant": set(),
        "attraction": set(),
        "budget": set(),
    }

    for c in constraints:
        category = c.variable.split(".")[0]
        if category == "transport":
            if c.value:
                entities["transport"].add(str(c.value))
        elif category == "hotel":
            val = c.value
            if c.variable == "hotel.star":
                entities["hotel"].add(f"star={val}")
            elif c.variable == "hotel.service":
                entities["hotel"].add(f"service={val}")
            elif c.variable == "hotel.name":
                entities["hotel"].add(str(val))
            else:
                entities["hotel"].add(str(val))
        elif category == "restaurant":
            if c.variable == "restaurant.tag":
                entities["restaurant"].add(f"tag={c.value}")
            elif c.variable == "restaurant.name":
                entities["restaurant"].add(str(c.value))
            else:
                entities["restaurant"].add(str(c.value))
        elif category == "attraction":
            if isinstance(c.value, list):
                for v in c.value:
                    entities["attraction"].add(str(v))
            elif c.value:
                entities["attraction"].add(str(c.value))
        elif category == "budget":
            entities["budget"].add(f"max={c.value}")

    return entities


def _meta_accuracy(trip_meta: TripMeta, gt_meta: Dict[str, Any]) -> Dict[str, bool]:
    """Check if extracted trip metadata matches ground truth."""
    return {
        "origin": trip_meta.origin.lower() == gt_meta.get("org", "").lower(),
        "destinations": set(d.lower() for d in trip_meta.destinations)
            == set(d.lower() for d in gt_meta.get("dest", [])),
        "days": trip_meta.days == gt_meta.get("days", 0),
        "depart_date": trip_meta.depart_date == gt_meta.get("depart_date", ""),
        "return_date": trip_meta.return_date == gt_meta.get("return_date", ""),
        "people_number": trip_meta.people_number == gt_meta.get("people_number", 1),
        "room_number": trip_meta.room_number == gt_meta.get("room_number", 1),
    }


def evaluate_extraction(
    model: str,
    query_file: Path,
    max_samples: int = 0,
) -> Dict[str, Any]:
    """
    Run constraint extraction on queries and compare with ground truth.

    Returns summary with per-sample and aggregate metrics.
    """
    with open(query_file, "r", encoding="utf-8") as f:
        queries = json.load(f)

    if max_samples > 0:
        queries = queries[:max_samples]

    results = []

    for sample in queries:
        sid = sample["id"]
        query = sample["query"]
        gt_meta = sample["meta_info"]
        gt_hc = gt_meta.get("hard_constraints", {})

        print(f"Extracting constraints for sample {sid}...")
        try:
            trip_meta, constraints = extract_constraints(query, model)
        except Exception as e:
            print(f"  Error: {e}")
            results.append({"id": sid, "error": str(e)})
            continue

        # Compare metadata
        meta_acc = _meta_accuracy(trip_meta, gt_meta)

        # Compare entities
        gt_ents = _ground_truth_entities(gt_hc)
        ex_ents = _extracted_entities(constraints)

        category_scores = {}
        for cat in ("transport", "hotel", "restaurant", "attraction", "budget"):
            gt_set = gt_ents[cat]
            ex_set = ex_ents[cat]
            if not gt_set:
                category_scores[cat] = {"precision": 1.0, "recall": 1.0, "f1": 1.0}
                continue
            # Fuzzy match: check if extracted values contain ground-truth as substring
            matched = sum(1 for g in gt_set if any(g.lower() in e.lower() or e.lower() in g.lower() for e in ex_set))
            recall = matched / len(gt_set) if gt_set else 1.0
            precision = matched / len(ex_set) if ex_set else 0.0
            f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
            category_scores[cat] = {"precision": precision, "recall": recall, "f1": f1}

        results.append({
            "id": sid,
            "meta_accuracy": meta_acc,
            "meta_correct": sum(meta_acc.values()) / len(meta_acc),
            "category_scores": category_scores,
            "num_constraints_extracted": len(constraints),
            "num_gt_constraints": len(gt_hc),
            "constraints": constraints_to_dicts(constraints),
        })

    # Aggregate
    valid_results = [r for r in results if "error" not in r]
    if valid_results:
        avg_meta = sum(r["meta_correct"] for r in valid_results) / len(valid_results)
        avg_recall = {}
        for cat in ("transport", "hotel", "restaurant", "attraction", "budget"):
            scores = [r["category_scores"][cat]["recall"] for r in valid_results]
            avg_recall[cat] = sum(scores) / len(scores)
    else:
        avg_meta = 0.0
        avg_recall = {}

    return {
        "model": model,
        "num_samples": len(queries),
        "num_successful": len(valid_results),
        "avg_meta_accuracy": avg_meta,
        "avg_recall_by_category": avg_recall,
        "per_sample": results,
    }


def main():
    parser = argparse.ArgumentParser(description="Evaluate constraint extraction")
    parser.add_argument("--model", type=str, required=True)
    parser.add_argument("--language", type=str, default="en")
    parser.add_argument("--samples", type=int, default=0, help="Max samples (0=all)")
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()

    base_dir = Path(__file__).parent.parent
    query_file = base_dir / "data" / f"travelplanning_query_{args.language}.json"

    results = evaluate_extraction(args.model, query_file, args.samples)

    print(f"\n{'='*60}")
    print(f"Constraint Extraction Evaluation — {args.model}")
    print(f"{'='*60}")
    print(f"Samples: {results['num_successful']}/{results['num_samples']}")
    print(f"Avg meta accuracy: {results['avg_meta_accuracy']:.1%}")
    print(f"Avg recall by category:")
    for cat, score in results.get("avg_recall_by_category", {}).items():
        print(f"  {cat}: {score:.1%}")

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\nResults saved to {out_path}")


if __name__ == "__main__":
    main()
