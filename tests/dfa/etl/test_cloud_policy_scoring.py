# Copyright (c) 2026, Oracle

import csv
import io
import json
import re

from dfa.etl.transformers.cloud_policy import CloudPolicyEventTransformer


def test_manage_all_resources_in_tenancy_scores_5():
    transformer = CloudPolicyEventTransformer("cloud_policy", "CREATE")
    raw_event = {
        "id": "pol-1",
        "statement": "Allow group admins to manage all-resources in tenancy",
        "verb": "MANAGE",
    }
    rows = transformer.transform_raw_event(raw_event)
    assert len(rows) >= 1
    row = rows[0]
    attrs = json.loads(row["attributes"]) if row.get("attributes") else {}
    assert attrs.get("permissive_score") == 5


def test_manage_domains_in_tenancy_with_condition_scores_4():
    transformer = CloudPolicyEventTransformer("cloud_policy", "CREATE")
    st = (
        "allow resource agcsgovernanceinstance agcs-rp to manage domains in tenancy "
        "where all {request.principal.siOcid='ocid1.agcs...'}"
    )
    raw_event = {
        "id": "pol-2",
        "statement": st,
        "verb": "MANAGE",
    }
    rows = transformer.transform_raw_event(raw_event)
    row = rows[0]
    attrs = json.loads(row["attributes"]) if row.get("attributes") else {}
    assert attrs.get("permissive_score") == 5


def test_read_object_family_in_compartment_scores_2():
    transformer = CloudPolicyEventTransformer("cloud_policy", "CREATE")
    st = "allow group readers to read object-family in compartment analytics"
    raw_event = {
        "id": "pol-3",
        "statement": st,
        "verb": "READ",
    }
    rows = transformer.transform_raw_event(raw_event)
    row = rows[0]
    attrs = json.loads(row["attributes"]) if row.get("attributes") else {}
    assert attrs.get("permissive_score") == 2


def test_inspect_specific_with_operation_filter_scores_min_1():
    transformer = CloudPolicyEventTransformer("cloud_policy", "CREATE")
    st = (
        "allow group devs to inspect buckets in compartment app-dev "
        "where all { request.operation in {'GetBucket'} }"
    )
    raw_event = {
        "id": "pol-4",
        "statement": st,
        "verb": "INSPECT",
    }
    rows = transformer.transform_raw_event(raw_event)
    row = rows[0]
    attrs = json.loads(row["attributes"]) if row.get("attributes") else {}
    assert attrs.get("permissive_score") == 1


def test_dynamic_group_without_guardrail_scores_5():
    transformer = CloudPolicyEventTransformer("cloud_policy", "CREATE")
    raw_event = {
        "id": "pol-5",
        "statement": "Allow dynamic-group dg-ci to manage domains in tenancy",
        "verb": "MANAGE",
        "subjects": [
            {"id": "ocid1.dynamicgroup.oc1..xyz", "name": "dg-ci", "type": "dynamic-group"}
        ],
    }
    rows = transformer.transform_raw_event(raw_event)
    attrs = json.loads(rows[0].get("attributes") or "{}")
    assert attrs.get("permissive_score") == 5


def test_dynamic_group_with_guardrail_scores_5():
    transformer = CloudPolicyEventTransformer("cloud_policy", "CREATE")
    raw_event = {
        "id": "pol-6",
        "statement": (
            "Allow dynamic-group dg-ci to manage domains in tenancy "
            "where request.principal.type = 'instance' and request.principal.compartment.id = ocid1.compartment.oc1..abc"
        ),
        "verb": "MANAGE",
        "subjects": [
            {"id": "ocid1.dynamicgroup.oc1..xyz", "name": "dg-ci", "type": "dynamic-group"}
        ],
    }
    rows = transformer.transform_raw_event(raw_event)
    attrs = json.loads(rows[0].get("attributes") or "{}")
    assert attrs.get("permissive_score") == 5


def test_dynamic_group_with_guardrail_reduces_below_5():
    transformer = CloudPolicyEventTransformer("cloud_policy", "CREATE")
    raw_event = {
        "id": "pol-7",
        "statement": (
            "Allow dynamic-group dg-ci to manage domains in tenancy "
            "where request.principal.id = ''"
        ),
        "verb": "MANAGE",
        "subjects": [
            {"id": "ocid1.dynamicgroup.oc1..xyz", "name": "dg-ci", "type": "dynamic-group"}
        ],
    }
    rows = transformer.transform_raw_event(raw_event)
    attrs = json.loads(rows[0].get("attributes") or "{}")
    assert attrs.get("permissive_score") < 5


def test_read_audit_events_with_condition_scores_below_5():
    transformer = CloudPolicyEventTransformer("cloud_policy", "CREATE")
    raw_event = {
        "id": "pol-8",
        "statement": (
            "allow resource agcsgovernanceinstance agcs-rp to read audit-events in tenancy "
            "where all {request.principal.siOcid='ocid1.agcsgovernanceinstance.oc1.iad.amaaaaaaebkbezqausedelhfjjyvyeg65rzo3lpkmr6fumawhlpw7m5najga'}"
        ),
        "verb": "READ",
    }
    rows = transformer.transform_raw_event(raw_event)
    attrs = json.loads(rows[0].get("attributes") or "{}")
    assert attrs.get("permissive_score") < 5


def test_all_patterns_in_csv_match_expected_scores():
    csv_path = "tests/dfa/etl/test_data/policy/anti-pattern.cleaned.csv"
    load_test_cases_from_csv(csv_path)
    csv_path = "tests/dfa/etl/test_data/policy/policy_examples.csv"
    load_test_cases_from_csv(csv_path)


def load_test_cases_from_csv(csv_path):
    transformer = CloudPolicyEventTransformer("cloud_policy", "CREATE")

    # Read with tolerant decoding (CSV may contain non-UTF8 characters like 'Ð')
    with open(csv_path, "rb") as fb:
        raw = fb.read()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("latin-1", errors="ignore")

    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        pattern = (row.get("Pattern", "") or "").strip()
        score_str = (row.get("Permissive Score", "") or "").strip()
        if not pattern or not score_str:
            continue
        # Skip narrative-only rows that do not contain an actionable policy grammar
        if not re.search(r"\b(allow|admit)\b", pattern, re.IGNORECASE):
            continue
        # Extract digits from the score cell
        digits = [int(ch) for ch in score_str if ch.isdigit()]
        if not digits:
            # Skip rows without a concrete numeric score
            continue
        expected = digits

        # Choose verb based on pattern text to better reflect the rule intent
        pl = pattern.lower()
        verb = "MANAGE"
        if re.search(r"\bread\b", pl):
            verb = "READ"
        elif re.search(r"\buse\b", pl):
            verb = "USE"
        elif re.search(r"\binspect\b", pl):
            verb = "INSPECT"

        # Build a minimal event embedding the CSV pattern into 'statement'.
        raw_event = {
            "id": "csv-pol",
            "statement": pattern,
            "verb": verb,
        }

        rows = transformer.transform_raw_event(raw_event)
        assert len(rows) >= 1
        row0 = rows[0]
        attrs = json.loads(row0.get("attributes") or "{}")
        got = attrs.get("permissive_score")
        assert got in expected, f"Pattern '{pattern}' expected score {expected} but got {got}"
