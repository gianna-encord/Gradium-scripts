from encord import EncordUserClient
from encord.workflow.stages.review import ReviewStage
from datetime import datetime, timedelta
from tqdm.auto import tqdm

# -----------------------------
# CONFIGURATION
# -----------------------------
PROJECT_HASH = "60320d93-9495-402d-bb86-6a0300643615"
SSH_KEY_PATH = ""

ANNOTATOR_EMAIL = "annotator99_lambda@encord.ai"  # ← annotator to QC
TARGET_PRIORITY = 0.99                              # ← 0.0–1.0 (1.0 = 100 in platform UI)
LOOKBACK_DAYS   = 90                               # ← how far back to search logs
BUNDLE_SIZE     = 100                              # ← max 100 per bundle (platform limit)

ANNOTATE_STAGE_NAME = "Annotate 1"
REVIEW_STAGE_NAME   = "Review 1"

# -----------------------------
# CONNECT
# -----------------------------
encord_client = EncordUserClient.create_with_ssh_private_key(ssh_private_key_path=SSH_KEY_PATH)
project = encord_client.get_project(PROJECT_HASH)

# -----------------------------
# STEP 1: Find the Annotate 1 stage UUID
# -----------------------------
annotate_stage = None
for stage in project.workflow.stages:
    if stage.title == ANNOTATE_STAGE_NAME:
        annotate_stage = stage
        break

if annotate_stage is None:
    available = [s.title for s in project.workflow.stages]
    raise ValueError(f"Stage '{ANNOTATE_STAGE_NAME}' not found. Available: {available}")

print(f"Found '{ANNOTATE_STAGE_NAME}' stage UUID: {annotate_stage.uuid}")

# -----------------------------
# STEP 2: Use get_editor_logs() to find data units actioned by the annotator
#
# get_task_actions() requires a Service Account (org-level) key.
# get_editor_logs() works with a user key and captures SUBMIT_TASK events
# in the Label Editor, which tells us who submitted each task in Annotate 1.
# -----------------------------
end_time   = datetime.now()
start_time = end_time - timedelta(days=LOOKBACK_DAYS)

print(f"\nQuerying editor logs for '{ANNOTATOR_EMAIL}' in '{ANNOTATE_STAGE_NAME}' "
      f"(last {LOOKBACK_DAYS} days)...")

annotated_data_hashes = set()

# get_editor_logs() has a 30-day max range — chunk the lookback window
chunk_days = 30
window_end = end_time
while window_end > start_time:
    window_start = max(window_end - timedelta(days=chunk_days), start_time)
    for log in project.get_editor_logs(
        start_time=window_start,
        end_time=window_end,
        action="submit_task",
        actor_user_email=ANNOTATOR_EMAIL,
        workflow_stage_id=annotate_stage.uuid,
    ):
        annotated_data_hashes.add(str(log.data_unit_id))
    window_end = window_start

print(f"Found {len(annotated_data_hashes)} data units submitted by '{ANNOTATOR_EMAIL}' "
      f"in '{ANNOTATE_STAGE_NAME}'.")

if not annotated_data_hashes:
    print("No tasks found. Check the annotator email, stage name, or increase LOOKBACK_DAYS.")
    exit()

# -----------------------------
# STEP 3: Get label rows in Review 1 and raise priority on matches
# -----------------------------
label_rows = project.list_label_rows_v2(
    include_children=True,
    workflow_graph_node_title_eq=REVIEW_STAGE_NAME,
)

print(f"\nScanning {len(label_rows)} label rows in '{REVIEW_STAGE_NAME}'...")

updated, skipped = 0, 0

with project.create_bundle(bundle_size=BUNDLE_SIZE) as bundle:
    for lr in tqdm(label_rows, desc="Setting priorities"):
        if str(lr.data_hash) in annotated_data_hashes:
            lr.set_priority(TARGET_PRIORITY, bundle=bundle)
            print(f"  [QUEUED] {lr.data_title} → priority={TARGET_PRIORITY} ({int(TARGET_PRIORITY * 100)}/100)")
            updated += 1
        else:
            skipped += 1

print(f"\n✓ Done! Updated: {updated} tasks | Skipped: {skipped} tasks")
print(f"Matched tasks in '{REVIEW_STAGE_NAME}' now have priority {TARGET_PRIORITY} ({int(TARGET_PRIORITY * 100)}/100 in platform UI).")