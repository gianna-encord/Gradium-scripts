
from encord import EncordUserClient
from encord.constants.enums import DataType
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv

SSH_PATH = ""
PROJECT_1_ID = "45dfb4ba-6a45-4e97-8bc0-620f66995dbc"
PROJECT_2_ID = "17d0c142-1be8-4762-9830-975def610bba"

OVERALL_PREFERENCE_HASH = "PlTiHnML"
NATURALNESS_HASH = "9IbNLYph"
EXPRESSIVENESS_HASH = "OZ650m99"
AUDIO_QUALITY_HASH = "YxxO4bjn"

# Filter to specific annotators, or None for all
FILTER_ANNOTATORS = ["annotator11_lambda@encord.ai"]


def extract_rating_value(answer):
    if not answer:
        return None
    if hasattr(answer, 'value'):
        answer_str = answer.value
    elif hasattr(answer, 'label'):
        answer_str = answer.label
    else:
        answer_str = str(answer[0]) if isinstance(answer, list) else str(answer)
    for char in str(answer_str):
        if char.isdigit():
            return int(char)
    return None


def get_task_data_with_annotators(project_id, user_client):
    print(f"\nLoading project {project_id}...")
    project = user_client.get_project(project_id)

    parent_label_rows = [lr for lr in project.list_label_rows_v2() if lr.data_type == DataType.GROUP]
    print(f"  Found {len(parent_label_rows)} data groups")
    print(f"  Initialising {len(parent_label_rows)} parent label rows...")

    def init_lr(lr):
        try:
            lr.initialise_labels()
            return lr, None
        except Exception as e:
            return lr, e

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(init_lr, lr): lr for lr in parent_label_rows}
        for future in as_completed(futures):
            lr, err = future.result()
            if err:
                print(f"  Warning: failed to initialise {lr.data_hash}: {err}")

    all_child_hashes = []
    parent_to_children = {}
    for parent_lr in parent_label_rows:
        if hasattr(parent_lr, 'metadata') and hasattr(parent_lr.metadata, 'children'):
            child_hashes = [child.data_hash for child in parent_lr.metadata.children]
            all_child_hashes.extend(child_hashes)
            parent_to_children[parent_lr.data_hash] = child_hashes

    print(f"  Fetching {len(all_child_hashes)} children...")
    child_label_rows = project.list_label_rows_v2(data_hashes=all_child_hashes, include_children=True)
    child_lookup = {lr.data_hash: lr for lr in child_label_rows}

    task_data = {}
    task_to_annotator = {}

    for parent_lr in parent_label_rows:
        parent_id = parent_lr.data_hash

        annotator = None
        if hasattr(parent_lr, '__dict__'):
            for attr_name in dir(parent_lr):
                if 'edit' in attr_name.lower() or 'user' in attr_name.lower():
                    try:
                        val = getattr(parent_lr, attr_name)
                        if val and isinstance(val, str) and '@' in val:
                            annotator = val
                            break
                    except:
                        pass
        if not annotator and hasattr(parent_lr, 'label_row_metadata') and parent_lr.label_row_metadata:
            if hasattr(parent_lr.label_row_metadata, 'last_edited_by'):
                annotator = parent_lr.label_row_metadata.last_edited_by
        task_to_annotator[parent_id] = annotator if annotator else "Unknown"

        if parent_id not in parent_to_children or len(parent_to_children[parent_id]) != 2:
            continue

        child_hashes = parent_to_children[parent_id]
        child1_lr = child_lookup.get(child_hashes[0])
        child2_lr = child_lookup.get(child_hashes[1])
        if not child1_lr or not child2_lr:
            continue

        try:
            child1_lr.initialise_labels()
            child2_lr.initialise_labels()
        except:
            continue

        spaces = list(parent_lr.get_spaces())
        if len(spaces) != 2:
            continue

        tracks = []
        for space in spaces:
            track_data = {
                'naturalness': None,
                'expressiveness': None,
                'audio_quality': None,
                'objects': []   # list of (name, attr_val) to preserve duplicates
            }
            for ci in space.get_classification_instances():
                try:
                    for attr in ci.ontology_item.attributes:
                        answer = ci.get_answer(attr)
                        if attr.feature_node_hash == NATURALNESS_HASH:
                            track_data['naturalness'] = extract_rating_value(answer)
                        elif attr.feature_node_hash == EXPRESSIVENESS_HASH:
                            track_data['expressiveness'] = extract_rating_value(answer)
                        elif attr.feature_node_hash == AUDIO_QUALITY_HASH:
                            track_data['audio_quality'] = extract_rating_value(answer)
                except:
                    pass

            for obj in space.get_object_instances():
                try:
                    if obj.ontology_item and obj.ontology_item.attributes:
                        for attr in obj.ontology_item.attributes:
                            if type(attr).__name__ == "RadioAttribute":
                                answer = obj.get_answer(attr)
                                if answer:
                                    if isinstance(answer, list):
                                        for option in answer:
                                            if hasattr(option, 'value'):
                                                track_data['objects'].append((obj.object_name, option.value))
                                    elif hasattr(answer, 'value'):
                                        track_data['objects'].append((obj.object_name, answer.value))
                except:
                    pass

            tracks.append(track_data)

        overall_prefs = []
        for child_lr in [child1_lr, child2_lr]:
            pref = None
            try:
                for ci in child_lr.get_classification_instances():
                    if hasattr(ci, 'ontology_item') and ci.ontology_item:
                        for attr in ci.ontology_item.attributes:
                            if attr.feature_node_hash == OVERALL_PREFERENCE_HASH:
                                answer = ci.get_answer(attr)
                                pref = extract_rating_value(answer)
                                break
            except:
                pass
            overall_prefs.append(pref)

        task_data[parent_id] = {
            'data_title': parent_lr.data_title,
            'track_1': tracks[0],
            'track_2': tracks[1],
            'overall_preference': overall_prefs[0] or overall_prefs[1]
        }

    print(f"  Loaded {len(task_data)} tasks")
    return task_data, task_to_annotator


def fmt_track(track, label=""):
    """Return a list of display lines for one track's labels."""
    lines = []
    lines.append(f"      Naturalness  : {track['naturalness'] if track['naturalness'] is not None else '—'}")
    lines.append(f"      Expressiveness: {track['expressiveness'] if track['expressiveness'] is not None else '—'}")
    lines.append(f"      Audio Quality : {track['audio_quality'] if track['audio_quality'] is not None else '—'}")
    if track['objects']:
        lines.append(f"      Objects:")
        for name, val in sorted(track['objects']):
            lines.append(f"        • {name} → {val}")
    else:
        lines.append(f"      Objects       : none")
    return lines


def diff_marker(v1, v2):
    """Return *** if values differ, else empty string."""
    return "  <<<" if v1 != v2 else ""


def print_comparison(p1_data, p2_data, annotator, task_ids):
    print(f"\n{'='*90}")
    print(f"ANNOTATOR: {annotator}  ({len(task_ids)} tasks)")
    print(f"{'='*90}")

    for task_id in sorted(task_ids, key=lambda t: p1_data[t]['data_title']):
        t1 = p1_data[task_id]
        t2 = p2_data[task_id]
        title = t1['data_title']

        print(f"\n  {'─'*86}")
        print(f"  TASK: {title}")
        print(f"  ID  : {task_id}")
        print(f"  {'─'*86}")

        # Overall preference
        op1 = t1.get('overall_preference')
        op2 = t2.get('overall_preference')
        marker = diff_marker(op1, op2)
        print(f"  Overall Preference:  P1={op1 if op1 is not None else '—'}   P2={op2 if op2 is not None else '—'}{marker}")

        for track_key, label in [('track_1', 'TRACK 1'), ('track_2', 'TRACK 2')]:
            tr1 = t1[track_key]
            tr2 = t2[track_key]

            print(f"\n    {label}")
            print(f"    {'P1 (reference project)':<42}  {'P2 (annotator project)'}")
            print(f"    {'-'*82}")

            # Classifications side by side
            for criterion in ['naturalness', 'expressiveness', 'audio_quality']:
                v1 = tr1[criterion] if tr1[criterion] is not None else '—'
                v2 = tr2[criterion] if tr2[criterion] is not None else '—'
                marker = diff_marker(v1, v2)
                print(f"    {criterion.title():<20}  {str(v1):<22}  {str(v2)}{marker}")

            # Objects side by side
            objs1 = sorted(set(tr1['objects']))
            objs2 = sorted(set(tr2['objects']))
            set1 = set(tr1['objects'])
            set2 = set(tr2['objects'])
            all_objs = sorted(set1 | set2)

            if all_objs:
                print(f"    {'Objects':<20}  {'P1':<22}  {'P2'}")
                for obj in all_objs:
                    name, val = obj
                    in1 = "✓" if obj in set1 else "✗"
                    in2 = "✓" if obj in set2 else "✗"
                    marker = "  <<<" if (obj in set1) != (obj in set2) else ""
                    print(f"      {name} → {val:<32}  P1:{in1}   P2:{in2}{marker}")
            else:
                print(f"    Objects              (none in either)")


def export_csv(p1_data, p2_data, annotator_tasks, path="label_comparison.csv"):
    rows = []
    for annotator, task_ids in annotator_tasks.items():
        for task_id in task_ids:
            t1 = p1_data[task_id]
            t2 = p2_data[task_id]
            base = {
                'annotator': annotator,
                'task_id': task_id,
                'data_title': t1['data_title'],
                'p1_overall_preference': t1.get('overall_preference'),
                'p2_overall_preference': t2.get('overall_preference'),
                'overall_match': t1.get('overall_preference') == t2.get('overall_preference'),
            }
            for track_key in ['track_1', 'track_2']:
                tr1 = t1[track_key]
                tr2 = t2[track_key]
                for criterion in ['naturalness', 'expressiveness', 'audio_quality']:
                    base[f'p1_{track_key}_{criterion}'] = tr1[criterion]
                    base[f'p2_{track_key}_{criterion}'] = tr2[criterion]
                    base[f'{track_key}_{criterion}_match'] = tr1[criterion] == tr2[criterion]
                base[f'p1_{track_key}_objects'] = "; ".join(f"{n}→{v}" for n, v in sorted(set(tr1['objects'])))
                base[f'p2_{track_key}_objects'] = "; ".join(f"{n}→{v}" for n, v in sorted(set(tr2['objects'])))
                base[f'{track_key}_objects_match'] = set(tr1['objects']) == set(tr2['objects'])
            rows.append(base)

    if not rows:
        print("Nothing to export.")
        return

    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nExported {len(rows)} rows to {path}")


def main():
    client = EncordUserClient.create_with_ssh_private_key(
        ssh_private_key_path=SSH_PATH,
        domain="https://api.encord.com"
    )

    print("=" * 90)
    print("LABEL COMPARISON — SIDE BY SIDE")
    print("=" * 90)

    p1_data, _ = get_task_data_with_annotators(PROJECT_1_ID, client)
    p2_data, p2_annotators = get_task_data_with_annotators(PROJECT_2_ID, client)

    annotator_tasks = defaultdict(list)
    for tid, ann in p2_annotators.items():
        if ann != "Unknown" and tid in p1_data and tid in p2_data:
            if FILTER_ANNOTATORS is None or ann in FILTER_ANNOTATORS:
                annotator_tasks[ann].append(tid)

    for annotator in sorted(annotator_tasks.keys()):
        print_comparison(p1_data, p2_data, annotator, annotator_tasks[annotator])

    export_csv(p1_data, p2_data, annotator_tasks)


if __name__ == "__main__":
    main()