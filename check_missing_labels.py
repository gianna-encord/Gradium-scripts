from encord import EncordUserClient
from encord.objects.attributes import TextAttribute, RadioAttribute
from encord.workflow.stages.review import ReviewStage
from encord.exceptions import LabelRowError, OntologyError

from tqdm.auto import tqdm

PROJECT_HASH = "60320d93-9495-402d-bb86-6a0300643615"
SSH_KEY_PATH = ""

OUTPUT_FILE = "invalid_data_items_pt.txt"

# -----------------------------
# CONNECT
# -----------------------------
encord_client = EncordUserClient.create_with_ssh_private_key(ssh_private_key_path=SSH_KEY_PATH)
project = encord_client.get_project(PROJECT_HASH)
annotation_stage = project.workflow.get_stage(name="Review 1", type_=ReviewStage)

label_rows = project.list_label_rows_v2(
    include_children=True,
    workflow_graph_node_title_eq="Review 1",
)
ontology = project.ontology_structure

ontology_name_to_feature_node_hash = {}
for item in ontology.classifications:
    if item.title == 'Overall preference':
        if len(item.attributes[0].options) == 2:
            ontology_name_to_feature_node_hash[item.title.lower() + ' valid'] = item.feature_node_hash
        else:
            ontology_name_to_feature_node_hash[item.title.lower() + ' not valid'] = item.feature_node_hash
    else:
        ontology_name_to_feature_node_hash[item.title] = item.feature_node_hash

object_required_attributes = {}
for obj in ontology.objects:
    if obj.attributes:
        object_required_attributes[obj.title] = obj.attributes

open(OUTPUT_FILE, "w").close()
open("invalid_data_items_fr.txt", "w").close()


def write_issue(message: str):
    with open(OUTPUT_FILE, "a") as f:
        f.write(message + "\n")


def is_other_option_selected(obj_instance, attributes):
    """Check if any radio attribute has 'other' selected as its answer."""
    for attribute in attributes:
        if isinstance(attribute, RadioAttribute):
            try:
                answer = obj_instance.get_answer(attribute)
                if answer is not None:
                    # Check if the selected option title contains 'other'
                    option_title = getattr(answer, 'value', '') or ''
                    if 'other' in option_title.lower():
                        return True
            except Exception:
                pass
    return False


for lr in tqdm(label_rows, desc=f"Checking labels for '{project.title}'"):
    try:
        lr.initialise_labels()
    except (LabelRowError, OntologyError) as e:
        with open("invalid_data_items_fr.txt", "a") as f:
            f.write(f"{lr.data_title} has invalid ontology: {str(e)}\n")
        continue
    except Exception as e:
        with open("invalid_data_items_fr.txt", "a") as f:
            f.write(f"{lr.data_title} has unexpected error: {str(e)}\n")
        continue

    try:
        for i, space in enumerate(lr.get_spaces()):
            ci_feature_hash_to_ci = {ci.feature_hash: ci for ci in space.get_classification_instances()}

            # Check for invalid overall preference
            if ci_feature_hash_to_ci and ontology_name_to_feature_node_hash.get(
                'overall preference not valid') in ci_feature_hash_to_ci.keys():
                write_issue(f"{lr.data_title} has invalid preference classification")

            # Check for missing classification fields
            missing_fields = []
            for field in ['Naturalness', 'Expressiveness', 'Audio Quality']:
                if ontology_name_to_feature_node_hash.get(field) not in ci_feature_hash_to_ci.keys():
                    missing_fields.append(field)
            if missing_fields:
                write_issue(f"{lr.data_title} space {i} is missing classifications: {', '.join(missing_fields)}")

            # Check object instances for missing attribute answers
            for obj_instance in space.get_object_instances():
                obj_name = obj_instance.object_name

                if obj_name not in object_required_attributes:
                    continue

                attributes = object_required_attributes[obj_name]
                other_selected = is_other_option_selected(obj_instance, attributes)

                missing_attrs = []
                for attribute in attributes:
                    try:
                        # Skip free-text "Other (explain):" fields unless "Other" was selected
                        if isinstance(attribute, TextAttribute) and 'other' in attribute.name.lower():
                            if not other_selected:
                                continue  # not a real missing field

                        answer = obj_instance.get_answer(attribute)
                        if answer is None or answer == [] or answer == "":
                            missing_attrs.append(attribute.name)
                    except Exception:
                        missing_attrs.append(attribute.name)

                if missing_attrs:
                    write_issue(
                        f"{lr.data_title} space {i} — '{obj_name}' is missing attribute(s): {', '.join(missing_attrs)}"
                    )

    except Exception as e:
        write_issue(f"{lr.data_title} error processing spaces: {str(e)}")
        continue

print(f"\n✓ Finished checking {len(label_rows)} label rows")
print(f"Check '{OUTPUT_FILE}' for any issues found")