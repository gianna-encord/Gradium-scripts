from encord import EncordUserClient
from encord.exceptions import LabelRowError
import csv

# ---- USER INPUT ----
SSH_PATH = ""
PROJECT_ID = "0ded235a-5939-42b7-9f9a-e5523b774870"  # Change this to your project


def main():
    # Instantiate Encord client
    user_client = EncordUserClient.create_with_ssh_private_key(
        ssh_private_key_path=SSH_PATH,
        domain="https://api.encord.com",
    )

    # Get project
    project = user_client.get_project(PROJECT_ID)
    assert project is not None, f"Project with ID {PROJECT_ID} could not be loaded"

    # Get all label rows
    label_rows = project.list_label_rows_v2()
    assert label_rows, f"No label rows found in project {PROJECT_ID}"

    print(f"Checking {len(label_rows)} tasks for invalid label ranges...\n")

    # Track tasks with invalid ranges
    invalid_range_tasks = []
    other_errors = []
    successful_tasks = 0

    for i, label_row in enumerate(label_rows, 1):
        try:
            label_row.initialise_labels()
            successful_tasks += 1

            if i % 50 == 0:
                print(f"  Progress: {i}/{len(label_rows)}")

        except LabelRowError as e:
            error_message = str(e)

            # Check if it's an invalid range error
            if "is invalid" in error_message and "ms long" in error_message:
                invalid_range_tasks.append({
                    'data_title': label_row.data_title if hasattr(label_row, 'data_title') else 'N/A',
                    'label_hash': label_row.label_hash,
                    'data_hash': label_row.data_hash if hasattr(label_row, 'data_hash') else 'N/A',
                    'error': error_message,
                    'encord_url': f"https://app.encord.com/label_editor/{PROJECT_ID}?label={label_row.label_hash}"
                })
            else:
                # Some other LabelRowError
                other_errors.append({
                    'data_title': label_row.data_title if hasattr(label_row, 'data_title') else 'N/A',
                    'label_hash': label_row.label_hash,
                    'error': error_message
                })

        except Exception as e:
            # Unexpected error
            other_errors.append({
                'data_title': label_row.data_title if hasattr(label_row, 'data_title') else 'N/A',
                'label_hash': label_row.label_hash,
                'error': str(e)
            })

    # Print summary
    print(f"\n{'=' * 80}")
    print("SUMMARY")
    print(f"{'=' * 80}")
    print(f"Total tasks checked: {len(label_rows)}")
    print(f"✓ Successfully initialized: {successful_tasks}")
    print(f"❌ Invalid label ranges (extends beyond audio): {len(invalid_range_tasks)}")
    print(f"⚠️  Other errors: {len(other_errors)}")

    # Print details of invalid range tasks
    if invalid_range_tasks:
        print(f"\n{'=' * 80}")
        print("TASKS WITH INVALID LABEL RANGES")
        print(f"{'=' * 80}\n")

        for i, task in enumerate(invalid_range_tasks, 1):
            print(f"{i}. Task: {task['data_title']}")
            print(f"   Label hash: {task['label_hash']}")
            print(f"   Data hash: {task['data_hash']}")
            print(f"   Error: {task['error']}")
            print(f"   Fix here: {task['encord_url']}")
            print()

        # Export to CSV
        csv_filename = "invalid_label_ranges.csv"

        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['data_title', 'label_hash', 'data_hash', 'error', 'encord_url']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for task in invalid_range_tasks:
                writer.writerow(task)

        print(f"{'=' * 80}")
        print(f"✓ Exported {len(invalid_range_tasks)} tasks to: {csv_filename}")
        print(f"{'=' * 80}")
    else:
        print("\n✅ No invalid label ranges found!")

    # Print other errors if any
    if other_errors:
        print(f"\n{'=' * 80}")
        print("OTHER ERRORS ENCOUNTERED")
        print(f"{'=' * 80}\n")

        for i, task in enumerate(other_errors[:10], 1):  # Show first 10
            print(f"{i}. Task: {task['data_title']}")
            print(f"   Label hash: {task['label_hash']}")
            print(f"   Error: {task['error'][:100]}...")
            print()

        if len(other_errors) > 10:
            print(f"... and {len(other_errors) - 10} more errors")


if __name__ == "__main__":
    main()
