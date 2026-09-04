# Copyright (c) 2026, Oracle

import csv
import io
import json
import re

from dfa.etl.transformers.cloud_policy import CloudPolicyEventTransformer


def test_dynamic_group_manage_in_compartment_scores_four():
    transformer = CloudPolicyEventTransformer("cloud_policy", "CREATE")
    raw_event = {
        "id": "pol-8",
        "statement": ("allow dynamic-group to manage iam-family in compartment"),
        "verb": "MANAGE",
    }
    rows = transformer.transform_raw_event(raw_event)
    attrs = json.loads(rows[0].get("attributes") or "{}")
    assert attrs.get("permissive_score") == 4


def test_reviewed_policy_score_transitions():
    cases = [
        ("allow group admins to manage all-resources in compartment apps", 4),
        ("allow group readers to use objects in tenancy", 2),
        (
            "allow group readers to use objects in tenancy " "where target.compartment.name = /apps-*/",
            1,
        ),
        ("endorse group operators to use secret-family in tenancy external", 4),
        (
            "allow group operators to manage volume-family in tenancy " "where target.compartment.name = /apps-*/",
            3,
        ),
        ("allow dynamic-group workers to manage objects in tenancy", 5),
        ("allow service objectstorage to manage objects in tenancy", 5),
        (
            "endorse any-user to {OBJECT_READ} in any-tenancy " "where request.principal.type = 'workload'",
            5,
        ),
    ]

    transformer = CloudPolicyEventTransformer("cloud_policy", "CREATE")
    for statement, expected in cases:
        rows = transformer.transform_raw_event({"id": "reviewed-policy", "statement": statement})
        attrs = json.loads(rows[0].get("attributes") or "{}")
        assert attrs.get("permissive_score") == expected, statement


def test_unconditioned_tenancy_family_manage_grants():
    cases = [
        ("allow group agents-service to manage object-family in tenancy", 5),
        ("allow group core-service-group to manage repos in tenancy", 5),
        ("allow group service-managers to manage cloud-guard-family in tenancy", 5),
        ("allow group admins to manage recovery-service-family in tenancy", 5),
        ("allow group GBUDS-ITOps to manage ai-service-speech-family in tenancy", 5),
        ("allow group pipeline-serviceaccounts to manage object-family in tenancy", 5),
        ("allow group admins to manage serviceconnectors in tenancy", 4),
        ("allow group operators to manage key-family in tenancy", 5),
        ("allow group operators to manage objects in tenancy", 4),
        (
            "allow group agents-service to manage object-family in tenancy " "where target.compartment.name = /apps-*/",
            3,
        ),
    ]

    transformer = CloudPolicyEventTransformer("cloud_policy", "CREATE")
    for statement, expected in cases:
        rows = transformer.transform_raw_event({"id": "service-policy", "statement": statement})
        attrs = json.loads(rows[0].get("attributes") or "{}")
        assert attrs.get("permissive_score") == expected, statement


def test_every_parsed_score_has_a_reason():
    statements = [
        "allow group readers to use objects in tenancy",
        "allow group readers to inspect buckets in compartment apps",
        "allow group operators to manage key-family in tenancy",
        "allow group operators to manage objects in tenancy where target.compartment.name = 'apps'",
    ]

    transformer = CloudPolicyEventTransformer("cloud_policy", "CREATE")
    for statement in statements:
        rows = transformer.transform_raw_event({"id": "reason-policy", "statement": statement})
        attrs = json.loads(rows[0].get("attributes") or "{}")
        assert attrs.get("permissive_score") in {1, 2, 3, 4, 5}, statement
        assert attrs.get("reasons"), statement
        assert "Base score" in attrs["reasons"], statement


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
        expected = {1, 2} if any(score in (1, 2) for score in digits) else set(digits)

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
        assert got in expected, f"Pattern '{pattern}' expected score {sorted(expected)} but got {got}"
