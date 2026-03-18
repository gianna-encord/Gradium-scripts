from encord import EncordUserClient
from encord.constants.enums import DataType
from encord.objects.metadata import DataGroupMetadata
from tqdm.auto import tqdm

SSH_KEY_PATH = ""

# Add your project IDs here
PROJECT_IDS = [
    "5c8a9426-ce19-423d-b1e5-184e435c963b",
]

# -----------------------------
# CONNECT
# -----------------------------
encord_client = EncordUserClient.create_with_ssh_private_key(
    ssh_private_key_path=SSH_KEY_PATH,
    domain="https://api.encord.com",
)

# -----------------------------
# PROCESS EACH PROJECT
# -----------------------------
for project_id in PROJECT_IDS:
    project = encord_client.get_project(project_id)
    print(f"\nProject: {project.title} ({project_id})")
    print("-" * 60)

    label_rows = list(project.list_label_rows_v2(include_children=True))
    print(f"  Loaded {len(label_rows)} label rows")

    # Build lookup of data_hash -> label row for child rows
    # No initialise_labels() call needed — duration is a property on the row itself
    child_lr_by_hash = {
        lr.data_hash: lr
        for lr in label_rows
        if lr.data_type != DataType.GROUP
    }

    project_total_seconds = 0.0
    file_count = 0
    missing_count = 0

    group_rows = [lr for lr in label_rows if lr.data_type == DataType.GROUP]

    for lr in tqdm(group_rows, desc="  Scanning groups"):
        try:
            lr.initialise_labels()  # only needed on the GROUP row for metadata
        except Exception:
            continue

        if not isinstance(lr.metadata, DataGroupMetadata):
            continue

        for child in lr.metadata.children:
            child_lr = child_lr_by_hash.get(child.data_hash)

            if child_lr is None:
                missing_count += 1
                continue

            # Access duration directly without initialising the child label row
            duration_s = getattr(child_lr, "duration", None)

            if duration_s is not None:
                project_total_seconds += float(duration_s)
                file_count += 1
            else:
                missing_count += 1

    project_hours = project_total_seconds / 3600
    project_minutes = project_total_seconds / 60

    print(f"  Audio files found     : {file_count}")
    print(f"  Files missing duration: {missing_count}")
    print(f"  Total audio duration  : {project_hours:.2f} hours ({project_minutes:.1f} minutes)")