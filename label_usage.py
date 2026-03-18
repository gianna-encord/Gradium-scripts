from collections import defaultdict
from encord import EncordUserClient
from encord.exceptions import LabelRowError

# ---- USER INPUT ----
SSH_PATH = ""
PROJECT_ID = "1d6dcff2-fbc4-4ff1-8201-d3132a7dfc53"

# Objects to exclude from attribute breakdown
EXCLUDE_FROM_ATTRIBUTES = {"Word"}


def _extract_answer_value(answer):
    """Extract a human-readable string from an answer object."""
    try:
        if answer is None:
            return None
        if isinstance(answer, list):
            parts = []
            for item in answer:
                try:
                    parts.append(item.value)
                except Exception:
                    parts.append(str(item))
            return ", ".join(parts) if parts else None
        try:
            return answer.value
        except AttributeError:
            val = str(answer)
            return val if val != "" else None
    except Exception:
        return None


def main():
    user_client = EncordUserClient.create_with_ssh_private_key(
        ssh_private_key_path=SSH_PATH,
        domain="https://api.encord.com",
    )

    project = user_client.get_project(PROJECT_ID)
    assert project is not None, f"Project with ID {PROJECT_ID} could not be loaded"

    label_rows = project.list_label_rows_v2()
    assert label_rows, f"No label rows found in project {PROJECT_ID}"

    initialized_rows = []
    failed_count = 0

    print(f"Initializing {len(label_rows)} label rows...")

    for i, label_row in enumerate(label_rows, 1):
        try:
            label_row.initialise_labels()
            initialized_rows.append(label_row)
            if i % 50 == 0:
                print(f"  Progress: {i}/{len(label_rows)}")
        except LabelRowError:
            failed_count += 1
        except Exception:
            failed_count += 1

    total_tasks = len(initialized_rows)

    print(f"\n✓ Successfully initialized: {total_tasks} label rows")
    print(f"✗ Failed (ontology mismatch): {failed_count} label rows\n")

    label_to_task_count = defaultdict(int)
    attribute_to_task_count = defaultdict(int)

    print("Scanning for labels in data group spaces...")

    for label_row in initialized_rows:
        labels_in_this_task = set()
        attributes_in_this_task = set()

        try:
            for space in label_row.get_spaces():
                try:
                    for obj in space.get_object_instances():
                        label_name = obj.object_name
                        labels_in_this_task.add(label_name)

                        if label_name in EXCLUDE_FROM_ATTRIBUTES:
                            continue

                        try:
                            for attribute in obj.ontology_item.attributes:
                                try:
                                    answer = obj.get_answer(attribute)
                                    value = _extract_answer_value(answer)
                                    if value is not None:
                                        attributes_in_this_task.add((label_name, attribute.name, value))
                                except Exception:
                                    pass
                        except Exception:
                            pass

                except Exception:
                    pass

                try:
                    for clf in space.get_classification_instances():
                        label_name = clf.classification_name
                        labels_in_this_task.add(label_name)
                except Exception:
                    pass

        except Exception:
            pass

        for label_name in labels_in_this_task:
            label_to_task_count[label_name] += 1

        for key in attributes_in_this_task:
            attribute_to_task_count[key] += 1

    sorted_labels = sorted(label_to_task_count.items(), key=lambda kv: kv[1], reverse=True)

    grouped = defaultdict(lambda: defaultdict(int))
    for (label_name, attr_name, value), count in attribute_to_task_count.items():
        grouped[label_name][value] += count

    print("\n" + "=" * 70)
    print("LABEL USAGE WITH ATTRIBUTE BREAKDOWN")
    print(f"Total tasks: {total_tasks}")
    print("=" * 70)

    if not sorted_labels:
        print("No labels found in any tasks")
    else:
        for label_name, task_count in sorted_labels:
            pct = (task_count / total_tasks * 100) if total_tasks > 0 else 0
            print(f"\n* {label_name} - {task_count} ({pct:.1f}% of tasks)")

            if label_name in grouped:
                attr_values = sorted(grouped[label_name].items(), key=lambda x: x[1], reverse=True)
                for value, count in attr_values:
                    attr_pct = (count / total_tasks * 100) if total_tasks > 0 else 0
                    print(f"   * {value} - {count} ({attr_pct:.1f}% of tasks)")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()