from encord import EncordUserClient
from encord.objects import Classification
from collections import defaultdict

SSH_PATH = ""
PROJECT_IDS = ["68f4e6e3-2cd4-4ebd-aab2-69827d34651d", "a2c606ba-0a4d-47b2-950c-0f716b98e7f7", "a2075a20-aaa3-4836-ba41-6d80de6a253d", "29e3bb28-14d1-49e4-879a-06f2cf6c9fcf", "aeeba4d4-c5c3-4438-902f-471c54928f47", "45dfb4ba-6a45-4e97-8bc0-620f66995dbc", "ec8ee4bb-0f79-4c67-9139-7e3fedf6df14", "188bf6a5-72a8-423f-ba29-47937919b064", "3c8f265d-a47f-451b-9afc-76aeccafb687", "af13862e-874c-49ee-868c-49027b81d772", "f3a82dd1-ed74-4723-a5b0-c7f5ff55a2fb", "f392bddb-6e6c-4692-84d1-1f5708336b9a", "0ded235a-5939-42b7-9f9a-e5523b774870", "73aa1b38-1c4f-4964-af32-2a5ee3fa9d9a", "60320d93-9495-402d-bb86-6a0300643615"]  # Add all your Gradium project IDs
CLASSIFICATION_TITLE = "overall preference"  # Exact name from ontology

user_client = EncordUserClient.create_with_ssh_private_key(ssh_private_key_path=SSH_PATH)

mismatched_tasks = []

for project_id in PROJECT_IDS:
    project = user_client.get_project(project_id)
    ontology = project.ontology_structure

    # Fetch all label rows; include_children=True ensures Data Group children are included
    all_rows = project.list_label_rows_v2(include_children=True)

    # Only process Data Group parent rows (group_hash is None for parents)
    data_group_parents = [row for row in all_rows if row.group_hash is None]
    print(f"Project {project_id}: {len(data_group_parents)} data groups found")

    # Use bundles for efficient initialisation
    with project.create_bundle() as bundle:
        for lr in data_group_parents:
            lr.initialise_labels(bundle=bundle)

    for lr in data_group_parents:
        spaces = lr.get_spaces()
        answers_per_space = {}

        for space in spaces:
            layout_key = space.metadata.layout_key
            for annotation in space.get_annotations(type_="classification"):
                ci = annotation.classification_instance
                if ci.classification_name == CLASSIFICATION_TITLE:
                    try:
                        answer = ci.get_answer()
                        answers_per_space[layout_key] = answer.value if answer else None
                    except Exception:
                        answers_per_space[layout_key] = None

        # Check if answers differ across spaces
        unique_answers = set(v for v in answers_per_space.values() if v is not None)
        if len(unique_answers) > 1:
            mismatched_tasks.append({
                "project_id": project_id,
                "data_title": lr.data_title,
                "label_hash": lr.label_hash,
                "answers_per_space": answers_per_space,
            })

print(f"\n=== Tasks with mismatched 'overall preference rating' across spaces ===")
print(f"Total mismatched tasks: {len(mismatched_tasks)}\n")
for task in mismatched_tasks:
    print(f"Project: {task['project_id']}")
    print(f"  Task: {task['data_title']} (label_hash: {task['label_hash']})")
    print(f"  Answers: {task['answers_per_space']}")
    print()