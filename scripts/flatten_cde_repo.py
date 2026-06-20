#!/usr/bin/env python3
"""Flatten NIH CDE Repository JSON into a TSV loadable by ddharmon.

Usage:
    python scripts/flatten_cde_repo.py data/cde/All-CDEs.json data/cde/all_cdes_flat.tsv
    python scripts/flatten_cde_repo.py data/cde/NIH-endorsed-CDEs.json data/cde/nih_endorsed_flat.tsv

Then load in ddharmon:
    dd = load_dictionary(
        "data/cde/all_cdes_flat.tsv",
        cohort_name="NIH_CDE",
        variable_name="designation",
        field_id="tinyId",
        description="definition",
        question_text="question_text",
        data_type="datatype",
        value_encoding="permissible_values",
        category="classification",
        standard_code="concept_codes",
        embed_variable_name=True,
    )
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path


def flatten_cde(record: dict) -> dict:
    """Flatten a single CDE record into a flat dict for TSV export."""

    # designation — primary name
    designations = record.get("designations", [])
    primary_name = designations[0].get("designation", "") if designations else ""

    # question_text — designation tagged as "Preferred Question Text"
    pqt = ""
    for d in designations:
        if "Preferred Question Text" in d.get("tags", []):
            pqt = d.get("designation", "")
            break

    # definition
    definitions = record.get("definitions", [])
    definition = definitions[0].get("definition", "") if definitions else ""

    # value domain
    vd = record.get("valueDomain", {})
    datatype = vd.get("datatype", "")

    # permissible values — format as "value=meaning | value=meaning"
    pvs = vd.get("permissibleValues", [])
    pv_parts = []
    for pv in pvs:
        val = pv.get("permissibleValue", "")
        meaning = pv.get("valueMeaningName", "")
        if meaning and meaning != val:
            pv_parts.append(f"{val}={meaning}")
        else:
            pv_parts.append(val)
    permissible_values = " | ".join(pv_parts)

    # classification — first path as category
    cls_parts = []
    for c in record.get("classification", []):
        steward = c.get("stewardOrg", {}).get("name", "")
        for e in c.get("elements", []):
            cls_parts.append(f"{steward} > {e.get('name', '')}")
    classification = "; ".join(cls_parts) if cls_parts else ""

    # concept codes from dataElementConcept, objectClass, property
    concept_codes = []
    for field in ("dataElementConcept", "objectClass", "property"):
        for c in record.get(field, {}).get("concepts", []):
            origin = c.get("origin", "")
            origin_id = c.get("originId", "")
            if origin and origin_id:
                concept_codes.append(f"{origin}:{origin_id}")

    # external IDs (LOINC, NINDS, BRICS, etc.)
    external_ids = []
    for i in record.get("ids", []):
        source = i.get("source", "")
        id_val = i.get("id", "")
        if source and id_val:
            external_ids.append(f"{source}:{id_val}")

    # steward org
    steward = record.get("stewardOrg", {}).get("name", "")

    # registration
    reg = record.get("registrationState", {})
    reg_status = reg.get("registrationStatus", "")

    # NIH endorsed flag
    nih_endorsed = record.get("nihEndorsed", False)

    return {
        "tinyId": record.get("tinyId", ""),
        "designation": primary_name,
        "question_text": pqt,
        "definition": definition,
        "datatype": datatype,
        "permissible_values": permissible_values,
        "classification": classification,
        "concept_codes": "; ".join(concept_codes),
        "external_ids": "; ".join(external_ids),
        "steward_org": steward,
        "registration_status": reg_status,
        "nih_endorsed": str(nih_endorsed),
    }


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <input.json> <output.tsv>")
        sys.exit(1)

    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])

    with open(input_path) as f:
        records = json.load(f)

    fieldnames = [
        "tinyId",
        "designation",
        "question_text",
        "definition",
        "datatype",
        "permissible_values",
        "classification",
        "concept_codes",
        "external_ids",
        "steward_org",
        "registration_status",
        "nih_endorsed",
    ]

    rows = [flatten_cde(r) for r in records]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)

    print(f"Flattened {len(rows)} CDEs to {output_path}")


if __name__ == "__main__":
    main()
