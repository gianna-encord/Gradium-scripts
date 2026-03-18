"""
Copy ONLY the "Word" object instances with their "Transcript #transcript" text attribute
from one Encord project to another (audio modality, data groups).

Requirements:
    pip install encord tqdm

Prerequisites:
    - Both projects share the same data units (matched by data_hash).
    - Both projects have the same "Word" object + "Transcript #transcript" attribute.
    - Source project has pre-labels on the MAIN branch.
"""

from __future__ import annotations

from encord import EncordUserClient
from encord.constants.enums import DataType
from encord.objects import Object
from encord.objects.attributes import TextAttribute
from tqdm import tqdm

# ── CONFIG ────────────────────────────────────────────────────────────────────
SSH_KEY_PATH        = ""
SOURCE_PROJECT_HASH = "45dfb4ba-6a45-4e97-8bc0-620f66995dbc"
TARGET_PROJECT_HASH = "17d0c142-1be8-4762-9830-975def610bba"

WORD_OBJECT_TITLE     = "Word"
TRANSCRIPT_ATTR_TITLE = "Transcript #transcript"

# "skip"      – leave target rows that already have Word labels untouched
# "overwrite" – clear existing Word instances first, then copy
OVERWRITE_STRATEGY = "skip"
# ─────────────────────────────────────────────────────────────────────────────


def is_audio_child(row) -> bool:
    """Return True for leaf-level audio rows (not data group parents)."""
    return row.data_type == DataType.AUDIO


def initialise_rows(rows: list, project) -> None:
    """
    Initialise label rows safely for audio data groups.
    - Rows that already have a label_hash → bundled (fast).
    - Rows with no label_hash (never opened) → one-by-one to avoid bulk-create conflicts.
    """
    labelled   = [r for r in rows if r.label_hash is not None]
    unlabelled = [r for r in rows if r.label_hash is None]

    if labelled:
        with project.create_bundle() as bundle:
            for row in labelled:
                row.initialise_labels(bundle=bundle)

    for row in unlabelled:
        try:
            row.initialise_labels()
        except Exception as e:
            print(f"  [WARN] Could not initialise '{row.data_title}': {e}")


def copy_word_transcripts(src_row, tgt_row, tgt_word_obj: Object,
                          src_word_obj: Object, src_transcript_attr,
                          tgt_transcript_attr) -> int:
    """Copy all Word instances (with transcript) from src → tgt. Returns count."""
    copied = 0
    for src_inst in src_row.get_object_instances(filter_ontology_object=src_word_obj):
        transcript_text = src_inst.get_answer(attribute=src_transcript_attr)

        tgt_inst = tgt_word_obj.create_instance()

        for annotation in src_inst.get_annotations():
            tgt_inst.set_for_frames(
                coordinates=annotation.coordinates,
                frames=annotation.frame,
                confidence=annotation.confidence,
                manual_annotation=annotation.manual_annotation,
            )

        if transcript_text is not None:
            tgt_inst.set_answer(answer=transcript_text, attribute=tgt_transcript_attr)

        tgt_row.add_object_instance(tgt_inst)
        copied += 1

    return copied


def main() -> None:
    # ── Authenticate ──────────────────────────────────────────────────────────
    client      = EncordUserClient.create_with_ssh_private_key(ssh_private_key_path=SSH_KEY_PATH)
    src_project = client.get_project(SOURCE_PROJECT_HASH)
    tgt_project = client.get_project(TARGET_PROJECT_HASH)

    # ── Validate ontology ─────────────────────────────────────────────────────
    src_word_obj      = src_project.ontology_structure.get_child_by_title(WORD_OBJECT_TITLE, type_=Object)
    tgt_word_obj      = tgt_project.ontology_structure.get_child_by_title(WORD_OBJECT_TITLE, type_=Object)
    src_transcript    = src_word_obj.get_child_by_title(TRANSCRIPT_ATTR_TITLE, type_=TextAttribute)
    tgt_transcript    = tgt_word_obj.get_child_by_title(TRANSCRIPT_ATTR_TITLE, type_=TextAttribute)

    print(f"✓ '{WORD_OBJECT_TITLE}' found — src: {src_word_obj.feature_node_hash}, tgt: {tgt_word_obj.feature_node_hash}")
    print(f"✓ '{TRANSCRIPT_ATTR_TITLE}' found — src: {src_transcript.feature_node_hash}, tgt: {tgt_transcript.feature_node_hash}")

    # ── Fetch rows — children only (audio leaf rows, not data group parents) ──
    print("\nFetching source rows (audio children only)…")
    src_rows = [r for r in src_project.list_label_rows_v2(include_children=True)
                if is_audio_child(r) and r.label_hash is not None]

    print("Fetching target rows (audio children only)…")
    tgt_rows = [r for r in tgt_project.list_label_rows_v2(include_children=True)
                if is_audio_child(r)]

    print(f"  Source audio rows with labels : {len(src_rows)}")
    print(f"  Target audio rows             : {len(tgt_rows)}")

    # ── Match by data_hash ────────────────────────────────────────────────────
    tgt_lookup = {r.data_hash: r for r in tgt_rows}
    matched_pairs = []
    for src_row in src_rows:
        tgt_row = tgt_lookup.get(src_row.data_hash)
        if tgt_row is None:
            print(f"  [WARN] No target match for '{src_row.data_title}' — skipping.")
            continue
        matched_pairs.append((src_row, tgt_row))

    if not matched_pairs:
        print("No matching audio data units found. Exiting.")
        return

    print(f"\nMatched {len(matched_pairs)} audio data unit(s).")

    # ── Initialise rows ───────────────────────────────────────────────────────
    print("Initialising source rows…")
    initialise_rows([s for s, _ in matched_pairs], src_project)
    print("Initialising target rows…")
    initialise_rows([t for _, t in matched_pairs], tgt_project)

    # ── Copy Word + Transcript ────────────────────────────────────────────────
    total_copied = skipped_rows = 0

    for src_row, tgt_row in tqdm(matched_pairs, desc="Copying"):
        src_instances = src_row.get_object_instances(filter_ontology_object=src_word_obj)
        if not src_instances:
            continue

        tgt_instances = tgt_row.get_object_instances(filter_ontology_object=tgt_word_obj)
        if tgt_instances:
            if OVERWRITE_STRATEGY == "skip":
                print(f"  [SKIP] '{tgt_row.data_title}' already has Word labels.")
                skipped_rows += 1
                continue
            elif OVERWRITE_STRATEGY == "overwrite":
                for inst in tgt_instances:
                    tgt_row.remove_object_instance(inst)

        n = copy_word_transcripts(src_row, tgt_row, tgt_word_obj,
                                  src_word_obj, src_transcript, tgt_transcript)
        tgt_row.save()
        total_copied += n

    print(f"\nDone. Word instances copied: {total_copied} | Rows skipped: {skipped_rows}")


if __name__ == "__main__":
    main()