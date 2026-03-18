"""
Annotator Bias Detection Script for Gradium / Encord
=====================================================
Calculates per-project label distribution averages and flags individual
annotators whose label usage deviates +/- 30% from the project average.

SETUP:
  pip install encord schedule

USAGE:
  - Fill in SSH_PATH and PROJECT_IDS below.
  - Run directly (python bias_detection.py) for a one-off report.
  - Set SCHEDULE_MODE = True to run automatically twice per day.

OUTPUT:
  - Prints a report to stdout.
  - Saves a timestamped JSON report to REPORT_OUTPUT_DIR.
"""

import json
import os
import schedule
import time
from collections import defaultdict
from datetime import datetime

from encord import EncordUserClient

# ─── CONFIG ────────────────────────────────────────────────────────────────────

SSH_PATH = ""
PROJECT_IDS = [
    "60320d93-9495-402d-bb86-6a0300643615",
]
BIAS_THRESHOLD = 0.30          # Flag if annotator deviates by more than 30%
BUNDLE_SIZE = 100
REPORT_OUTPUT_DIR = "./bias_reports"
SCHEDULE_MODE = False          # Set True to run 2x per day automatically

# Encord domain — use "https://api.us.encord.com" for US platform users
ENCORD_DOMAIN = "https://api.encord.com"

# ─── CORE LOGIC ────────────────────────────────────────────────────────────────

def connect():
    return EncordUserClient.create_with_ssh_private_key(
        ssh_private_key_path=SSH_PATH,
        domain=ENCORD_DOMAIN,
    )


def get_label_distribution(project):
    """
    Returns:
        overall_dist  : {label_name: percentage_of_all_tasks}
        annotator_dist: {annotator_email: {label_name: percentage_of_their_tasks}}
        annotator_totals: {annotator_email: total_task_count}
        total_tasks   : int
    """
    # label_counts[annotator_email][label_name] = count
    label_counts = defaultdict(lambda: defaultdict(int))
    overall_counts = defaultdict(int)
    annotator_totals = defaultdict(int)

    label_rows = project.list_label_rows_v2(
        label_statuses=None  # include all statuses; filter to SUBMITTED if preferred
    )

    with project.create_bundle(bundle_size=BUNDLE_SIZE) as bundle:
        for lr in label_rows:
            lr.initialise_labels(bundle=bundle)

    for lr in label_rows:
        annotator = lr.last_edited_by  # email of the annotator who last edited
        if not annotator:
            continue

        task_labels = set()
        for ci in lr.get_classification_instances():
            try:
                answer = ci.get_answer()
                label_name = answer.value if hasattr(answer, "value") else str(answer)
            except Exception:
                label_name = ci.classification_name  # fallback to class name

            task_labels.add(label_name)

        # Also capture object instance names (for projects using objects not classifications)
        for oi in lr.get_object_instances():
            task_labels.add(oi.object_name)

        if not task_labels:
            continue

        annotator_totals[annotator] += 1
        for label in task_labels:
            label_counts[annotator][label] += 1
            overall_counts[label] += 1

    total_tasks = sum(annotator_totals.values())
    if total_tasks == 0:
        return {}, {}, {}, 0

    overall_dist = {
        label: count / total_tasks
        for label, count in overall_counts.items()
    }

    annotator_dist = {}
    for annotator, counts in label_counts.items():
        n = annotator_totals[annotator]
        annotator_dist[annotator] = {
            label: count / n for label, count in counts.items()
        }

    return overall_dist, annotator_dist, dict(annotator_totals), total_tasks


def detect_bias(overall_dist, annotator_dist, annotator_totals, threshold=BIAS_THRESHOLD):
    """
    Compares each annotator's label distribution to the project average.
    Flags labels where the annotator's % deviates by more than `threshold`.

    Returns a list of flagged results.
    """
    flags = []
    all_labels = set(overall_dist.keys())

    for annotator, dist in annotator_dist.items():
        annotator_flags = []
        for label in all_labels:
            avg_pct = overall_dist.get(label, 0.0)
            ann_pct = dist.get(label, 0.0)
            deviation = ann_pct - avg_pct

            if abs(deviation) > threshold:
                annotator_flags.append({
                    "label": label,
                    "annotator_pct": round(ann_pct * 100, 1),
                    "average_pct": round(avg_pct * 100, 1),
                    "deviation_pct": round(deviation * 100, 1),
                    "direction": "OVER" if deviation > 0 else "UNDER",
                })

        if annotator_flags:
            flags.append({
                "annotator": annotator,
                "total_tasks": annotator_totals.get(annotator, 0),
                "flags": sorted(annotator_flags, key=lambda x: abs(x["deviation_pct"]), reverse=True),
            })

    return sorted(flags, key=lambda x: len(x["flags"]), reverse=True)


def run_report():
    print(f"\n{'='*60}")
    print(f"  ANNOTATOR BIAS REPORT  —  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")

    client = connect()
    os.makedirs(REPORT_OUTPUT_DIR, exist_ok=True)
    full_report = {"generated_at": datetime.now().isoformat(), "projects": []}

    for project_id in PROJECT_IDS:
        project = client.get_project(project_id)
        project_title = project.title

        print(f"\n📁 Project: {project_title} ({project_id})")
        print("-" * 50)

        overall_dist, annotator_dist, annotator_totals, total_tasks = get_label_distribution(project)

        if total_tasks == 0:
            print("  No annotated tasks found.")
            continue

        # Print project-wide averages
        print(f"  Total tasks (across all annotators): {total_tasks}")
        print(f"  {'Annotators:':<20} {len(annotator_dist)}")
        print(f"\n  📊 Project-wide label averages:")
        for label, pct in sorted(overall_dist.items(), key=lambda x: -x[1]):
            print(f"     {label:<30} {pct*100:>5.1f}%")

        # Detect and print flags
        flags = detect_bias(overall_dist, annotator_dist, annotator_totals)

        if not flags:
            print(f"\n  ✅ No annotators flagged for bias (threshold: ±{BIAS_THRESHOLD*100:.0f}%)")
        else:
            print(f"\n  ⚠️  Flagged annotators (threshold: ±{BIAS_THRESHOLD*100:.0f}%):")
            for entry in flags:
                print(f"\n     👤 {entry['annotator']}  ({entry['total_tasks']} tasks)")
                for f in entry["flags"]:
                    direction_icon = "🔺" if f["direction"] == "OVER" else "🔻"
                    print(
                        f"        {direction_icon} {f['label']:<28} "
                        f"annotator: {f['annotator_pct']:>5.1f}%  "
                        f"avg: {f['average_pct']:>5.1f}%  "
                        f"(deviation: {f['deviation_pct']:+.1f}%)"
                    )

        # Build project report object
        project_report = {
            "project_id": project_id,
            "project_title": project_title,
            "total_tasks": total_tasks,
            "label_averages": {k: round(v * 100, 2) for k, v in overall_dist.items()},
            "annotator_summaries": [
                {
                    "annotator": a,
                    "total_tasks": annotator_totals[a],
                    "label_distribution": {k: round(v * 100, 2) for k, v in annotator_dist[a].items()},
                }
                for a in annotator_dist
            ],
            "flagged_annotators": flags,
        }
        full_report["projects"].append(project_report)

    # Save JSON report
    filename = f"bias_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    report_path = os.path.join(REPORT_OUTPUT_DIR, filename)
    with open(report_path, "w") as f:
        json.dump(full_report, f, indent=2)

    print(f"\n💾 Report saved to: {report_path}")
    print(f"{'='*60}\n")


# ─── SCHEDULER ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run_report()  # Always run once immediately on startup

    if SCHEDULE_MODE:
        print("⏰ Scheduler active — running twice daily at 08:00 and 20:00.")
        schedule.every().day.at("08:00").do(run_report)
        schedule.every().day.at("20:00").do(run_report)

        while True:
            schedule.run_pending()
            time.sleep(60)