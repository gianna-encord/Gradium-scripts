from encord.user_client import EncordUserClient
from encord.workflow import AgentStage

# ── Edit these values ──────────────────────────────────────────────────
PRIVATE_KEY_PATH  = "/Users/giannacipponeri/source/keys/encord-gianna-new-accelerate-private-key.ed25519"
PROJECT_HASH      = "60320d93-9495-402d-bb86-6a0300643615"
AGENT_STAGE_NAME  = "QA Check"
APPROVE_PATHWAY   = "approve"
REJECT_PATHWAY    = "reject"
HIGH_PRIORITY     = 1.0
# ───────────────────────────────────────────────────────────────────────

# ── Ontology ───────────────────────────────────────────────────────────
OBJECTS_WITH_REQUIRED_ATTRS = {
    "2MOEj3CT": ("Word Issue",         "fzsyd30s", "Word-related error"),
    "tJbr0xgT": ("Cadence Issue",      "9RLtjDhK", "Cadence issue"),
    "KAY7cqm3": ("Intonation Issue",   "qsYCZXxP", "Intonation Issue"),
    "HqMebGiv": ("Unclean start/end",  "xLlclcnU", "Unclean start or end"),
    "sj4dmbCK": ("Script Issue",       "kUgHIQu5", "Script Issue"),
}

PER_TRACK_REQUIRED_CLS = {
    "ufA6gXP5": "Naturalness",
    "xaJ2rpWW": "Expressiveness",
    "7xJOaGVw": "Audio Quality",
}

GLOBAL_CLS_FNH  = "8pRhIcKZ"
GLOBAL_CLS_NAME = "Overall preference"
# ───────────────────────────────────────────────────────────────────────


def is_answer_empty(answer) -> bool:
    """Return True if an attribute answer is effectively unset."""
    if answer is None:
        return True
    if isinstance(answer, str) and answer.strip() == "":
        return True
    try:
        if hasattr(answer, "__iter__") and not isinstance(answer, str):
            return len(list(answer)) == 0
    except Exception:
        pass
    return False


def check_space(space, space_label: str) -> list[str]:
    """
    Validate one Label Space (= one audio track).
    Checks:
      1. Any drawn object with a required attribute must have it answered.
      2. All three required per-track classifications must be present & answered.
    """
    problems = []

    # ── 1. Objects: required attributes ───────────────────────────────
    for obj_inst in space.get_object_instances():
        obj_fnh = obj_inst.feature_hash
        if obj_fnh not in OBJECTS_WITH_REQUIRED_ATTRS:
            continue

        obj_name, req_attr_fnh, attr_name = OBJECTS_WITH_REQUIRED_ATTRS[obj_fnh]
        for attribute in obj_inst.ontology_item.attributes:
            if attribute.feature_node_hash == req_attr_fnh:
                answer = obj_inst.get_answer(attribute)
                if is_answer_empty(answer):
                    problems.append(f"{space_label} - '{obj_name}' missing '{attr_name}'")
                break

    # ── 2. Per-track classifications ───────────────────────────────────
    answered_cls_fnhs = {
        ci.feature_hash
        for ci in space.get_classification_instances()
        if not is_answer_empty(ci.get_answer())
    }
    for fnh, cls_name in PER_TRACK_REQUIRED_CLS.items():
        if fnh not in answered_cls_fnhs:
            problems.append(f"Track {space_label} - missing {cls_name}")

    return problems


def check_global_classification(label_row) -> list[str]:
    """
    Check that the global 'Overall preference' classification is answered
    on at least one location (label row itself or any space).
    """
    for ci in label_row.get_classification_instances():
        if ci.feature_hash == GLOBAL_CLS_FNH and not is_answer_empty(ci.get_answer()):
            return []

    for space in label_row.get_spaces():
        for ci in space.get_classification_instances():
            if ci.feature_hash == GLOBAL_CLS_FNH and not is_answer_empty(ci.get_answer()):
                return []

    return [f"Task: missing global '{GLOBAL_CLS_NAME}'"]


def check_global_classification_consistency(label_row) -> list[str]:
    """
    Check that the 'Overall preference' classification has the SAME answer
    across all Label Spaces in the task.

    Even though the classification is marked Global, the SDK stores it
    independently per space in Data Groups — so annotators can set
    different values per track. This function detects that mismatch.
    """
    space_answers: dict[str, str] = {}  # layout_key -> answer value

    for space in label_row.get_spaces():
        layout_key = space.metadata.layout_key or space.space_id
        for ci in space.get_classification_instances():
            if ci.feature_hash == GLOBAL_CLS_FNH:
                answer = ci.get_answer()
                if not is_answer_empty(answer):
                    # answer.value for radio options, str() fallback for safety
                    val = answer.value if hasattr(answer, "value") else str(answer)
                    space_answers[layout_key] = val

    if len(space_answers) < 2:
        return []  # Nothing to compare (0 or 1 space answered)

    unique_answers = set(space_answers.values())
    if len(unique_answers) > 1:
        detail = ", ".join(f"Track {k}={v}" for k, v in sorted(space_answers.items()))
        return [f"'{GLOBAL_CLS_NAME}' mismatch across tracks ({detail})"]

    return []


def main():
    user_client = EncordUserClient.create_with_ssh_private_key(
        ssh_private_key_path=PRIVATE_KEY_PATH
    )
    project = user_client.get_project(PROJECT_HASH)
    print(f"Project loaded: {project.title}")

    agent_stage = project.workflow.get_stage(name=AGENT_STAGE_NAME, type_=AgentStage)
    tasks = list(agent_stage.get_tasks())
    print(f"Tasks in agent stage: {len(tasks)}")

    for task in tasks:
        print(f"\nProcessing task — data_hash={task.data_hash}, title={task.data_title}")

        label_rows = project.list_label_rows_v2(data_hashes=[task.data_hash])
        if not label_rows:
            print("  No label rows found — rejecting.")
            task.proceed(pathway_name=REJECT_PATHWAY)
            continue

        lr = label_rows[0]
        lr.initialise_labels()

        spaces = lr.get_spaces()
        print(f"  Label spaces found: {len(spaces)}")
        for s in spaces:
            print(f"    space_id={s.space_id}, layout_key={s.metadata.layout_key}, "
                  f"file_name={s.metadata.file_name}")

        all_problems: list[str] = []

        # ── Validate each audio track (Label Space) independently ──────
        for space in spaces:
            space_label = f"Track {space.metadata.layout_key}"
            all_problems.extend(check_space(space, space_label))

        # ── Validate global classification is present ───────────────────
        all_problems.extend(check_global_classification(lr))

        # ── Validate global classification is consistent across tracks ──
        all_problems.extend(check_global_classification_consistency(lr))

        # ── Reject or approve ───────────────────────────────────────────
        if all_problems:
            bullet_list = "\n".join(f"  • {p}" for p in all_problems)
            issue_comment = " | ".join(all_problems)
            print(f"  Rejecting. Issues:\n{bullet_list}")

            task.issues.add_file_issue(issue_comment, [])

            with project.create_bundle() as bundle:
                lr.set_priority(HIGH_PRIORITY, bundle=bundle)

            task.proceed(pathway_name=REJECT_PATHWAY)
        else:
            print("  All checks passed — approving.")
            task.proceed(pathway_name=APPROVE_PATHWAY)

    print("\nDone.")


if __name__ == "__main__":
    main()