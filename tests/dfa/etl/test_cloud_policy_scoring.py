# Copyright (c) 2026, Oracle

import json
import os

from dfa.etl.transformers.cloud_policy import CloudPolicyEventTransformer


def _tx():
    # threshold default 5; ensure set
    os.environ.setdefault("DFA_POLICY_OVERLY_PERMISSIVE_THRESHOLD", "5")
    return CloudPolicyEventTransformer("cloud_policy", "CREATE")


def test_manage_all_resources_in_tenancy_scores_5():
    transformer = _tx()
    raw_event = {
        "id": "pol-1",
        "statement": "Allow group admins to manage all-resources in tenancy",
        "verb": "MANAGE",
        "resourceTypes": ["all-resources"],
        "location": "tenancy",
    }
    rows = transformer.transform_raw_event(raw_event)
    assert len(rows) >= 1
    row = rows[0]
    attrs = json.loads(row["attributes"]) if row.get("attributes") else {}
    assert attrs.get("permissive_score") == 5


def test_manage_domains_in_tenancy_with_condition_scores_4():
    transformer = _tx()
    st = (
        "allow resource agcsgovernanceinstance agcs-rp to manage domains in tenancy "
        "where all {request.principal.siOcid='ocid1.agcs...'}"
    )
    raw_event = {
        "id": "pol-2",
        "statement": st,
        "verb": "MANAGE",
        "resourceTypes": ["domains"],
        "location": "tenancy",
    }
    rows = transformer.transform_raw_event(raw_event)
    row = rows[0]
    attrs = json.loads(row["attributes"]) if row.get("attributes") else {}
    assert attrs.get("permissive_score") == 4


def test_read_object_family_in_compartment_scores_3():
    transformer = _tx()
    st = "allow group readers to read object-family in compartment analytics"
    raw_event = {
        "id": "pol-3",
        "statement": st,
        "verb": "READ",
        "resourceTypes": ["object-family"],
        "location": {"compartment": "analytics"},
    }
    rows = transformer.transform_raw_event(raw_event)
    row = rows[0]
    attrs = json.loads(row["attributes"]) if row.get("attributes") else {}
    assert attrs.get("permissive_score") == 3


def test_inspect_specific_with_operation_filter_scores_min_1():
    transformer = _tx()
    st = (
        "allow group devs to inspect buckets in compartment app-dev "
        "where all { request.operation in {'GetBucket'} }"
    )
    raw_event = {
        "id": "pol-4",
        "statement": st,
        "verb": "INSPECT",
        "resourceTypes": ["buckets"],
        "location": {"compartment": "app-dev"},
    }
    rows = transformer.transform_raw_event(raw_event)
    row = rows[0]
    attrs = json.loads(row["attributes"]) if row.get("attributes") else {}
    assert attrs.get("permissive_score") == 1
