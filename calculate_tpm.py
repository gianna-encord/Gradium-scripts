import csv
from datetime import datetime
from collections import defaultdict
from encord import EncordUserClient

# -----------------------------
# CONFIGURATION
# -----------------------------
SSH_PATH = ""
API_DOMAIN = "https://api.encord.com"
CSV_FILE = ("gradium_file_length_es.csv")
START_DATE = datetime(2025, 2, 26)
END_DATE = datetime(2027, 12, 31)

# -----------------------------
# CONNECT AND LOAD DATA
# -----------------------------
print("Connecting to Encord...")
user_client = EncordUserClient.create_with_ssh_private_key(
    ssh_private_key_path=SSH_PATH,
    domain=API_DOMAIN,
)

print("Loading CSV data...")
task_durations = {}  # (project_hash, group_title) -> total duration in seconds
unique_projects = set()

with open(CSV_FILE, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        project_hash = row["project_hash"]
        title = row["title"]  # now the group title e.g. datagroup_...
        duration_seconds = float(row["duration"])

        key = (project_hash, title)
        task_durations[key] = task_durations.get(key, 0.0) + duration_seconds
        unique_projects.add(project_hash)

print(f"Loaded {len(task_durations)} tasks from {len(unique_projects)} projects\n")

# -----------------------------
# FETCH TIME SPENT FROM ENCORD
# -----------------------------
print("Fetching timer data from Encord...")

# Store: (project, task) -> total time spent
project_task_time = defaultdict(float)
# Store: (project, task, user_email) -> time spent by that user
project_task_user_time = defaultdict(float)

for project_id in unique_projects:
    project = user_client.get_project(project_id)
    time_entries = list(project.list_time_spent(start=START_DATE, end=END_DATE))

    for entry in time_entries:
        task_hash = entry.data_title
        time_spent_seconds = entry.time_spent_seconds
        user_email = entry.user_email

        # Aggregate total time per task
        key = (project_id, task_hash)
        project_task_time[key] += float(time_spent_seconds)

        # Aggregate time per task per user
        user_key = (project_id, task_hash, user_email)
        project_task_user_time[user_key] += float(time_spent_seconds)

print(f"Retrieved timer data for {len(project_task_time)} tasks\n")
# -----------------------------
# CALCULATE METRICS
# -----------------------------

# Overall metrics
total_time_spent_seconds = 0.0
total_audio_duration_minutes = 0.0
matched_tasks = 0

# By project: project_id -> {time_spent, audio_duration, task_count}
by_project = defaultdict(lambda: {'time_spent': 0.0, 'audio_duration': 0.0, 'task_count': 0})

# By annotator within project: (project_id, user_email) -> {time_spent, audio_duration, task_count}
by_project_annotator = defaultdict(lambda: {'time_spent': 0.0, 'audio_duration': 0.0, 'task_count': 0})

# For determining which user spent the most time on each task (last actioned)
task_primary_user = {}  # (project, task) -> user_email with most time

for (project_hash, title), duration_seconds in task_durations.items():
    key = (project_hash, title)

    if key in project_task_time:
        time_spent = project_task_time[key]
        audio_minutes = duration_seconds / 60.0

        # Overall totals
        total_time_spent_seconds += time_spent
        total_audio_duration_minutes += audio_minutes
        matched_tasks += 1

        # By project
        by_project[project_hash]['time_spent'] += time_spent
        by_project[project_hash]['audio_duration'] += audio_minutes
        by_project[project_hash]['task_count'] += 1

        # Find the user who spent the most time on this task
        max_time = 0
        primary_user = None
        for (proj, task, user), user_time in project_task_user_time.items():
            if proj == project_hash and task == title:
                if user_time > max_time:
                    max_time = user_time
                    primary_user = user

        if primary_user:
            task_primary_user[key] = primary_user

            # By annotator within project
            annotator_key = (project_hash, primary_user)
            by_project_annotator[annotator_key]['time_spent'] += time_spent
            by_project_annotator[annotator_key]['audio_duration'] += audio_minutes
            by_project_annotator[annotator_key]['task_count'] += 1

# -----------------------------
# DISPLAY RESULTS
# -----------------------------
print("=" * 80)
print("OVERALL RESULTS")
print("=" * 80)

if total_audio_duration_minutes > 0:
    overall_tpm = (total_time_spent_seconds / 60.0) / total_audio_duration_minutes
    print(f"\n✅ Overall Average TPM: {overall_tpm:.2f} minutes per audio minute")
    print(f"   Tasks matched: {matched_tasks:,}")
    print(f"   Total work time: {total_time_spent_seconds / 3600:.2f} hours")
    print(f"   Total audio: {total_audio_duration_minutes / 60:.2f} hours")
else:
    print("❌ No matching tasks found")

# -----------------------------
# BY PROJECT
# -----------------------------
print("\n" + "=" * 80)
print("BREAKDOWN BY PROJECT")
print("=" * 80)

for project_id in sorted(by_project.keys()):
    stats = by_project[project_id]
    if stats['audio_duration'] > 0:
        tpm = (stats['time_spent'] / 60.0) / stats['audio_duration']
        print(f"\nProject: {project_id}")
        print(f"  TPM: {tpm:.2f} minutes/audio minute")
        print(f"  Tasks: {stats['task_count']}")
        print(f"  Work time: {stats['time_spent'] / 3600:.2f} hours")
        print(f"  Audio: {stats['audio_duration'] / 60:.2f} hours")

# -----------------------------
# BY ANNOTATOR WITHIN EACH PROJECT
# -----------------------------
print("\n" + "=" * 80)
print("BREAKDOWN BY ANNOTATOR (WITHIN EACH PROJECT)")
print("=" * 80)

for project_id in sorted(by_project.keys()):
    print(f"\n{'─' * 80}")
    print(f"Project: {project_id}")
    print(f"{'─' * 80}")

    # Get all annotators for this project
    project_annotators = [(proj, user) for (proj, user) in by_project_annotator.keys() if proj == project_id]

    if not project_annotators:
        print("  No annotator data available")
        continue

    # Sort by TPM (descending)
    annotator_stats = []
    for annotator_key in project_annotators:
        stats = by_project_annotator[annotator_key]
        if stats['audio_duration'] > 0:
            tpm = (stats['time_spent'] / 60.0) / stats['audio_duration']
            annotator_stats.append((annotator_key[1], tpm, stats))

    annotator_stats.sort(key=lambda x: x[1], reverse=True)

    for user_email, tpm, stats in annotator_stats:
        print(f"\n  Annotator: {user_email}")
        print(f"    TPM: {tpm:.2f} minutes/audio minute")
        print(f"    Tasks: {stats['task_count']}")
        print(f"    Work time: {stats['time_spent'] / 3600:.2f} hours")
        print(f"    Audio: {stats['audio_duration'] / 60:.2f} hours")

print("\n" + "=" * 80)