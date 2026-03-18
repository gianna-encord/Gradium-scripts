"""
Annotator Bias Detection Script for Gradium / Encord
=====================================================
Runs bias detection across multiple projects in parallel and outputs
a clean consolidated report.

Only tracks annotator-assigned issue labels (Word Issue, Cadence Issue,
Intonation Issue, Script Issue, etc.) — transcript "Word" labels and
generic ontology class names are excluded.

SETUP:
  pip install encord schedule

USAGE:
  - Fill in SSH_PATH and PROJECT_IDS below.
  - Run directly (python bias_detection.py) for a one-off report.
  - Set SCHEDULE_MODE = True to run automatically twice per day.
"""

import json
import os
import schedule
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from encord import EncordUserClient

# ─── CONFIG ────────────────────────────────────────────────────────────────────

SSH_PATH = ""

PROJECT_IDS = [
    "219e34bf-7f76-4942-8c2b-2bdd51def6df",
    "721f50b4-35ed-4f49-a3f0-2e601d1db4bb",
]

BIAS_THRESHOLD  = 0.20   # Flag if annotator deviates by more than 30%
BUNDLE_SIZE     = 100
REPORT_OUTPUT_DIR = "./bias_reports"
SCHEDULE_MODE   = False  # Set True to run 2x per day automatically
MAX_WORKERS     = 5      # Run all projects in parallel

ENCORD_DOMAIN = "https://api.encord.com"

# Transcript pre-labels and generic ontology names to ignore
EXCLUDED_LABELS = {
    "word", "object", "classification", "overall preference",
    "naturalness", "expressiveness", "audio quality",
}

# ─── HELPERS ───────────────────────────────────────────────────────────────────

def connect():
    return EncordUserClient.create_with_ssh_private_key(
        ssh_private_key_path=SSH_PATH,
        domain=ENCORD_DOMAIN,
    )


def is_issue_label(label_name: str) -> bool:
    return bool(label_name) and label_name.lower().strip() not in EXCLUDED_LABELS


def print_divider(char="─", width=70):
    print(char * width)


# ─── PER-PROJECT LOGIC ─────────────────────────────────────────────────────────

def get_label_distribution(project):
    label_counts    = defaultdict(lambda: defaultdict(int))
    overall_counts  = defaultdict(int)
    annotator_totals = defaultdict(int)

    label_rows = project.list_label_rows_v2(label_statuses=None)

    with project.create_bundle(bundle_size=BUNDLE_SIZE) as bundle:
        for lr in label_rows:
            lr.initialise_labels(bundle=bundle)

    for lr in label_rows:
        try:
            spaces = lr.get_spaces()
        except Exception:
            continue

        for space in spaces:
            try:
                for annotation in space.get_annotations(type_="object"):
                    try:
                        annotator  = annotation.created_by
                        label_name = annotation.object_instance.object_name if hasattr(annotation, 'object_instance') else None
                        if not annotator or not label_name or not is_issue_label(label_name):
                            continue
                        label_counts[annotator][label_name] += 1
                        overall_counts[label_name] += 1
                        annotator_totals[annotator] += 1
                    except Exception:
                        continue
            except Exception:
                pass

    total = sum(annotator_totals.values())
    if total == 0:
        return {}, {}, {}, 0

    overall_dist   = {l: c / total for l, c in overall_counts.items()}
    annotator_dist = {
        a: {l: c / annotator_totals[a] for l, c in counts.items()}
        for a, counts in label_counts.items()
    }
    return overall_dist, annotator_dist, dict(annotator_totals), total


def detect_bias(overall_dist, annotator_dist, annotator_totals, threshold=BIAS_THRESHOLD):
    flags = []
    for annotator, dist in annotator_dist.items():
        annotator_flags = []
        for label in overall_dist:
            avg_pct = overall_dist.get(label, 0.0)
            ann_pct = dist.get(label, 0.0)
            deviation = ann_pct - avg_pct
            if abs(deviation) > threshold:
                annotator_flags.append({
                    "label":         label,
                    "annotator_pct": round(ann_pct * 100, 1),
                    "average_pct":   round(avg_pct * 100, 1),
                    "deviation_pct": round(deviation * 100, 1),
                    "direction":     "OVER" if deviation > 0 else "UNDER",
                })
        if annotator_flags:
            flags.append({
                "annotator":       annotator,
                "total_instances": annotator_totals.get(annotator, 0),
                "flags":           sorted(annotator_flags, key=lambda x: abs(x["deviation_pct"]), reverse=True),
            })
    return sorted(flags, key=lambda x: len(x["flags"]), reverse=True)


def analyse_project(client, project_id):
    """Runs full analysis for one project. Called in parallel."""
    try:
        project = client.get_project(project_id)
        overall_dist, annotator_dist, annotator_totals, total = get_label_distribution(project)
        flags = detect_bias(overall_dist, annotator_dist, annotator_totals) if total > 0 else []
        return {
            "project_id":    project_id,
            "project_title": project.title,
            "total":         total,
            "overall_dist":  overall_dist,
            "annotator_dist": annotator_dist,
            "annotator_totals": annotator_totals,
            "flags":         flags,
            "error":         None,
        }
    except Exception as e:
        return {"project_id": project_id, "project_title": "?", "error": str(e)}


# ─── OUTPUT ────────────────────────────────────────────────────────────────────

def print_project_report(result, index, total_projects):
    pid   = result["project_id"]
    title = result["project_title"]

    print(f"\n  {'━'*66}")
    print(f"  PROJECT {index}/{total_projects}  │  {title}")
    print(f"  {pid}")
    print(f"  {'━'*66}")

    if result.get("error"):
        print(f"  ❌ Error: {result['error']}")
        return

    total         = result["total"]
    overall_dist  = result["overall_dist"]
    annotator_dist = result["annotator_dist"]
    flags         = result["flags"]

    if total == 0:
        print("  ℹ️  No issue-type annotation instances found.")
        return

    print(f"  Total issue instances : {total}")
    print(f"  Annotators            : {len(annotator_dist)}")

    # Label averages table
    print(f"\n  📊 Project-wide issue label averages:")
    for label, pct in sorted(overall_dist.items(), key=lambda x: -x[1]):
        bar   = "█" * int(pct * 40)
        print(f"     {label:<30} {pct*100:>5.1f}%  {bar}")

    # Flagged annotators
    if not flags:
        print(f"\n  ✅ No annotators flagged (threshold ±{BIAS_THRESHOLD*100:.0f}%)")
    else:
        print(f"\n  ⚠️  Flagged annotators (threshold ±{BIAS_THRESHOLD*100:.0f}%):")
        for entry in flags:
            print(f"\n     👤 {entry['annotator']}  ({entry['total_instances']} instances)")
            for f in entry["flags"]:
                icon = "🔺" if f["direction"] == "OVER" else "🔻"
                print(
                    f"        {icon} {f['label']:<28} "
                    f"annotator: {f['annotator_pct']:>5.1f}%  "
                    f"avg: {f['average_pct']:>5.1f}%  "
                    f"({f['deviation_pct']:+.1f}%)"
                )


def print_cross_project_summary(results):
    """Prints a summary table across all projects."""
    print(f"\n\n  {'═'*66}")
    print(f"  CROSS-PROJECT SUMMARY")
    print(f"  {'═'*66}")

    # Collect all annotators flagged across projects
    flagged_across = defaultdict(list)
    for r in results:
        if r.get("error") or not r.get("flags"):
            continue
        for entry in r["flags"]:
            flagged_across[entry["annotator"]].append({
                "project": r["project_title"],
                "flags":   len(entry["flags"]),
                "instances": entry["total_instances"],
            })

    if not flagged_across:
        print("\n  ✅ No annotators flagged across any project.")
        return

    print(f"\n  Annotators flagged in multiple projects:\n")
    multi = {a: v for a, v in flagged_across.items() if len(v) > 1}
    single = {a: v for a, v in flagged_across.items() if len(v) == 1}

    for annotator, projects in sorted(multi.items(), key=lambda x: -len(x[1])):
        print(f"  ⚠️  {annotator}  — flagged in {len(projects)} projects")
        for p in projects:
            print(f"       • {p['project'][:55]:<55} {p['flags']} flag(s), {p['instances']} instances")

    if single:
        print(f"\n  Annotators flagged in 1 project:")
        for annotator, projects in sorted(single.items()):
            p = projects[0]
            print(f"  •  {annotator:<45} {p['project'][:30]}")


# ─── MAIN REPORT ───────────────────────────────────────────────────────────────

def run_report():
    start = datetime.now()
    print(f"\n{'═'*70}")
    print(f"  ANNOTATOR BIAS REPORT  —  {start.strftime('%Y-%m-%d %H:%M')}")
    print(f"  Running {len(PROJECT_IDS)} projects in parallel (workers: {MAX_WORKERS})")
    print(f"{'═'*70}")

    client = connect()
    os.makedirs(REPORT_OUTPUT_DIR, exist_ok=True)

    # Run all projects in parallel
    results_map = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyse_project, client, pid): pid for pid in PROJECT_IDS}
        for future in as_completed(futures):
            pid = futures[future]
            results_map[pid] = future.result()
            print(f"  ✓ Finished: {results_map[pid].get('project_title', pid)}")

    # Print in original order
    results = [results_map[pid] for pid in PROJECT_IDS]
    for i, result in enumerate(results, 1):
        print_project_report(result, i, len(PROJECT_IDS))

    # Cross-project summary
    print_cross_project_summary(results)

    # Save JSON
    elapsed = (datetime.now() - start).seconds
    full_report = {
        "generated_at": start.isoformat(),
        "elapsed_seconds": elapsed,
        "projects": [
            {
                "project_id":    r["project_id"],
                "project_title": r["project_title"],
                "error":         r.get("error"),
                "total_instances": r.get("total", 0),
                "label_averages": {k: round(v * 100, 2) for k, v in r.get("overall_dist", {}).items()},
                "annotator_summaries": [
                    {
                        "annotator":    a,
                        "total_instances": r["annotator_totals"][a],
                        "label_distribution": {k: round(v * 100, 2) for k, v in r["annotator_dist"][a].items()},
                    }
                    for a in r.get("annotator_dist", {})
                ],
                "flagged_annotators": r.get("flags", []),
            }
            for r in results
        ],
    }

    filename    = f"bias_report_{start.strftime('%Y%m%d_%H%M%S')}.json"
    report_path = os.path.join(REPORT_OUTPUT_DIR, filename)
    with open(report_path, "w") as f:
        json.dump(full_report, f, indent=2)

    print(f"\n\n  💾 Report saved to: {report_path}  (completed in {elapsed}s)")
    print(f"{'═'*70}\n")


# ─── SCHEDULER ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run_report()

    if SCHEDULE_MODE:
        print("⏰ Scheduler active — running twice daily at 08:00 and 20:00.")
        schedule.every().day.at("08:00").do(run_report)
        schedule.every().day.at("20:00").do(run_report)
        while True:
            schedule.run_pending()
            time.sleep(60)