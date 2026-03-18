import csv
from encord import EncordUserClient
from encord.constants.enums import DataType
from encord.objects.metadata import DataGroupMetadata

SSH_PATH = ""
PROJECT_ID = "1d6dcff2-fbc4-4ff1-8201-d3132a7dfc53"
CSV_FILE = "gradium_file_length_en.csv"

user_client = EncordUserClient.create_with_ssh_private_key(
    ssh_private_key_path=SSH_PATH,
    domain="https://api.encord.com",
)

project = user_client.get_project(PROJECT_ID)
label_rows = list(project.list_label_rows_v2(include_children=True))

child_lr_by_hash = {
    lr.data_hash: lr
    for lr in label_rows
    if lr.data_type != DataType.GROUP
}

rows_to_write = []

for lr in label_rows:
    if lr.data_type == DataType.GROUP:
        lr.initialise_labels()

        print(f"\nData Group: {lr.data_title}")
        print("-" * 40)

        assert isinstance(lr.metadata, DataGroupMetadata)

        for child in lr.metadata.children:
            child_lr = child_lr_by_hash.get(child.data_hash)

            if child_lr is None:
                print(f"  Audio file : {child.name} — label row not found, skipping")
                continue

            child_lr.initialise_labels()
            duration_s = getattr(child_lr, "duration", None)

            if duration_s is not None:
                mins, secs = divmod(duration_s, 60)
                duration_str = f"{int(mins)}m {secs:.1f}s"
                print(f"  Audio file : {child.name}")
                print(f"  Duration   : {duration_str} ({duration_s}s)")

                rows_to_write.append({
                    "project_hash": PROJECT_ID,
                    "title": lr.data_title,  # <-- use the GROUP title, not child.name
                    "filename": child.name,
                    "duration": duration_s,
                })
            else:
                print(f"  Audio file : {child.name} — duration N/A, skipping")

# Write to CSV
with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["project_hash", "title", "filename", "duration"])
    writer.writeheader()
    writer.writerows(rows_to_write)

print(f"\nWrote {len(rows_to_write)} rows to {CSV_FILE}")