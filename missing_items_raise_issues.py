from encord import EncordUserClient
from tqdm.auto import tqdm

PROJECT_HASH = "f392bddb-6e6c-4692-84d1-1f5708336b9a"
SSH_KEY_PATH = "/Users/giannacipponeri/source/keys/encord-gianna-new-accelerate-private-key.ed25519"
INPUT_FILE = ("invalid_data_items_pt.txt")
PRIORITY = 1.0
DRY_RUN = False  # Set to False when ready to apply changes
TEST_TASK = None # Set to None to process all tasks

# -----------------------------
# CONNECT
# -----------------------------
encord_client = EncordUserClient.create_with_ssh_private_key(ssh_private_key_path=SSH_KEY_PATH)
project = encord_client.get_project(PROJECT_HASH)

# -----------------------------
# PARSE ISSUES FROM FILE (one entry per line, no deduplication)
# -----------------------------
issues = []  # list of (data_title, full_line)

with open(INPUT_FILE, "r") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue

        if " space " in line:
            data_title = line.split(" space ")[0].strip()
        elif " has " in line:
            data_title = line.split(" has ")[0].strip()
        elif " error " in line:
            data_title = line.split(" error ")[0].strip()
        else:
            continue

        issues.append((data_title, line))

print(f"Found {len(issues)} issues across tasks")
print(f"DRY RUN: {DRY_RUN}\n")

# -----------------------------
# FETCH LABEL ROWS
# -----------------------------
label_rows = project.list_label_rows_v2(
    include_children=True,
    workflow_graph_node_title_eq="Review 1",
)

lr_by_title = {lr.data_title: lr for lr in label_rows}

# Filter to test task if set
unique_titles = set(data_title for data_title, _ in issues)
if TEST_TASK:
    unique_titles = {TEST_TASK}
    issues = [(t, i) for t, i in issues if t == TEST_TASK]
    print(f"TEST MODE — only processing: {TEST_TASK}\n")

# -----------------------------
# SET PRIORITY (once per unique task)
# -----------------------------
print(f"Setting priority={PRIORITY} on {len(unique_titles)} unique tasks...\n")

for data_title in unique_titles:
    lr = lr_by_title.get(data_title)
    if lr is None:
        print(f"  ✗ Not found: {data_title}")
        continue
    if DRY_RUN:
        print(f"  [DRY RUN] Would set priority={PRIORITY} on {data_title}")
    else:
        try:
            lr.initialise_labels()
            lr.set_priority(priority=PRIORITY)
            print(f"  ✓ Priority set: {data_title}")
        except Exception as e:
            print(f"  ✗ Failed to set priority on {data_title}: {e}")

# -----------------------------
# RAISE ONE ISSUE PER LINE
# -----------------------------
print(f"\nRaising {len(issues)} issues...\n")

succeeded = 0
failed = 0
not_found = 0

for data_title, issue_text in tqdm(issues, desc="Raising issues"):
    lr = lr_by_title.get(data_title)

    if lr is None:
        print(f"  ✗ Not found: {data_title}")
        not_found += 1
        continue

    if DRY_RUN:
        print(f"  [DRY RUN] Would raise issue on {data_title}:\n    {issue_text}")
        succeeded += 1
        continue

    # Debug: print ALL non-private methods on lr and project
    print(f"  LR methods: {[m for m in dir(lr) if not m.startswith('_')]}")
    print(f"  Project methods: {[m for m in dir(project) if not m.startswith('_')]}")
    break  # only need to see this once

# -----------------------------
# SUMMARY
# -----------------------------
print(f"\n{'=' * 60}")
if DRY_RUN:
    print("DRY RUN COMPLETE — no changes were made")
print(f"✓ {'Would raise' if DRY_RUN else 'Raised'}:  {succeeded} issues")
print(f"✗ Not found: {not_found} tasks")
print(f"✗ Failed:    {failed} issues")