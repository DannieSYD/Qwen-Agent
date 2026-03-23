"""Analyze behavioral differences between Qwen3-30B and GPT-5.4 on travel planning trajectories."""
import json
import re
from collections import Counter, defaultdict

MODELS = {
    "qwen3-30b": "qwen3-30b-a3b-thinking-2507_en",
    "gpt-5.4": "gpt-5.4_en",
}
BASE = "/data1/dannie/projects/Qwen-Agent/benchmark/deepplanning/travelplanning/results"
IDS = ["id_0", "id_1", "id_2"]

INFO_TOOLS = {"recommend_restaurants", "query_restaurant_details", "query_attraction_details", "query_hotel_info"}

def load_trajectory(model_dir, tid):
    path = f"{BASE}/{model_dir}/trajectories/{tid}.json"
    with open(path) as f:
        return json.load(f)

def extract_tool_calls(traj):
    """Extract ordered list of tool calls from messages."""
    calls = []
    for msg in traj["messages"]:
        if msg.get("tool_calls"):
            for tc in msg["tool_calls"]:
                fn = tc["function"]["name"]
                args = tc["function"].get("arguments", "{}")
                calls.append({"name": fn, "arguments": args})
    return calls

def extract_tool_results(traj):
    """Extract tool result contents keyed by tool_call_id."""
    results = {}
    for msg in traj["messages"]:
        if msg.get("role") == "tool":
            results[msg.get("tool_call_id", "")] = {
                "name": msg.get("name", ""),
                "content": msg.get("content", ""),
            }
    return results

def get_entities_from_tool_results(traj):
    """Extract entity names from tool results (hotels, restaurants, attractions)."""
    entities = set()
    for msg in traj["messages"]:
        if msg.get("role") != "tool":
            continue
        content = msg.get("content", "")
        # Try to parse as JSON
        try:
            data = json.loads(content)
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        for key in ["name", "restaurant_name", "Attraction Name"]:
                            if key in item:
                                entities.add(item[key])
                        # Nested list (train results)
                        if isinstance(item, list):
                            for sub in item:
                                if isinstance(sub, dict) and "name" in sub:
                                    entities.add(sub["name"])
            elif isinstance(data, dict):
                for key in ["name", "restaurant_name", "Attraction Name"]:
                    if key in data:
                        entities.add(data[key])
        except (json.JSONDecodeError, TypeError):
            # Parse non-JSON tool results (attraction details format)
            for line in content.split("\n"):
                if "Attraction Name" in line:
                    name = line.split("：")[-1].strip() if "：" in line else line.split(":")[-1].strip()
                    if name:
                        entities.add(name)
                if "name" in line.lower() and ("restaurant" in line.lower() or "hotel" in line.lower()):
                    name = line.split("：")[-1].strip() if "：" in line else line.split(":")[-1].strip()
                    if name:
                        entities.add(name)
    return entities

def extract_plan_entities(plan_text):
    """Extract entity names mentioned in the final plan (hotels, restaurants, attractions)."""
    entities = set()
    if not plan_text:
        return entities
    for line in plan_text.split("\n"):
        # attraction lines
        if "| attraction |" in line:
            match = re.search(r'\| attraction \| (.+?),', line)
            if match:
                entities.add(match.group(1).strip())
        # meal lines
        if "| meal |" in line:
            match = re.search(r'\| meal \| (?:Lunch|Dinner), (.+?),', line)
            if match:
                entities.add(match.group(1).strip())
        # hotel/accommodation
        if line.startswith("Accommodation:") and "-" not in line.split(":")[1]:
            name = line.split(":")[1].split(",")[0].strip()
            if name and name != "-":
                entities.add(name)
    return entities

def check_info_before_plan(traj):
    """Check if info-gathering tools were called BEFORE the plan was generated."""
    plan_turn_idx = None
    for i, msg in enumerate(traj["messages"]):
        if msg.get("role") == "assistant" and "<plan>" in (msg.get("content") or ""):
            plan_turn_idx = i
            break

    tools_before_plan = []
    for i, msg in enumerate(traj["messages"]):
        if plan_turn_idx is not None and i >= plan_turn_idx:
            break
        if msg.get("tool_calls"):
            for tc in msg["tool_calls"]:
                tools_before_plan.append(tc["function"]["name"])

    result = {}
    for tool in ["recommend_restaurants", "query_restaurant_details", "query_attraction_details", "query_hotel_info"]:
        result[tool] = tool in tools_before_plan
    return result

def analyze_parallelism(traj):
    """Count how many tool calls are made per assistant turn (parallelism)."""
    counts = []
    for msg in traj["messages"]:
        if msg.get("role") == "assistant" and msg.get("tool_calls"):
            counts.append(len(msg["tool_calls"]))
    return counts

def main():
    for model_label, model_dir in MODELS.items():
        print("=" * 80)
        print(f"MODEL: {model_label} ({model_dir})")
        print("=" * 80)

        for tid in IDS:
            traj = load_trajectory(model_dir, tid)
            calls = extract_tool_calls(traj)

            print(f"\n--- {tid} ---")
            print(f"Total tool calls: {len(calls)}")

            # Count by name
            counter = Counter(c["name"] for c in calls)
            print(f"Tool call counts:")
            for name, count in counter.most_common():
                print(f"  {name}: {count}")

            # Call order sequence
            seq = [c["name"] for c in calls]
            print(f"Call order sequence:")
            for i, name in enumerate(seq):
                print(f"  [{i+1}] {name}")

            # Parallelism
            par = analyze_parallelism(traj)
            print(f"Calls per assistant turn: {par}")
            print(f"  Max parallel calls in one turn: {max(par) if par else 0}")
            print(f"  Number of tool-calling turns: {len(par)}")

            # Info tools before plan
            info_check = check_info_before_plan(traj)
            print(f"Info tools called BEFORE plan generation:")
            for tool, called in info_check.items():
                print(f"  {tool}: {'YES' if called else 'NO'}")

            # Hallucination check
            tool_entities = get_entities_from_tool_results(traj)
            plan_entities = extract_plan_entities(traj.get("final_plan", ""))

            # Check for entities in plan not found in tool results
            # Use substring matching since names may differ slightly
            potentially_hallucinated = []
            for pe in plan_entities:
                found = False
                for te in tool_entities:
                    if pe.lower() in te.lower() or te.lower() in pe.lower():
                        found = True
                        break
                if not found:
                    potentially_hallucinated.append(pe)

            print(f"Entities in final plan: {plan_entities}")
            print(f"Potentially hallucinated entities (not in tool results): {potentially_hallucinated if potentially_hallucinated else 'None'}")

        print()

    # Summary comparison
    print("=" * 80)
    print("SUMMARY COMPARISON")
    print("=" * 80)

    for model_label, model_dir in MODELS.items():
        total_calls = []
        total_turns = []
        max_parallel = []
        all_tools = Counter()

        for tid in IDS:
            traj = load_trajectory(model_dir, tid)
            calls = extract_tool_calls(traj)
            total_calls.append(len(calls))
            par = analyze_parallelism(traj)
            total_turns.append(len(par))
            max_parallel.append(max(par) if par else 0)
            for c in calls:
                all_tools[c["name"]] += 1

        print(f"\n{model_label}:")
        print(f"  Avg tool calls per trajectory: {sum(total_calls)/len(total_calls):.1f}")
        print(f"  Avg tool-calling turns: {sum(total_turns)/len(total_turns):.1f}")
        print(f"  Max parallel calls in one turn: {max(max_parallel)}")
        print(f"  Tool usage across all trajectories:")
        for name, count in all_tools.most_common():
            print(f"    {name}: {count}")

if __name__ == "__main__":
    main()
