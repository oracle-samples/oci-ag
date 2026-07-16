# Copyright (c) 2026, Oracle

from dfa.etl.transformers.cloud_group import CloudGroupEventTransformer
from dfa.etl.transformers.policy import PolicyEventTransformer
from dfa.etl.transformers.role import RoleEventTransformer


def test_role_with_empty_access_bundles_emits_base_role():
    transformer = RoleEventTransformer("role", "CREATE")

    rows = transformer.transform_raw_event({"id": "role-1", "accessBundleIds": []})

    assert len(rows) == 1
    assert rows[0]["id"] == "role-1"
    assert rows[0]["access_bundle_id"] == ""


def test_policy_with_empty_rules_emits_base_policy():
    transformer = PolicyEventTransformer("policy", "CREATE")

    rows = transformer.transform_raw_event({"id": "policy-1", "policyRules": []})

    assert len(rows) == 1
    assert rows[0]["id"] == "policy-1"
    assert rows[0]["policy_rule_id"] == ""


def test_cloud_group_with_empty_memberships_emits_one_base_group():
    transformer = CloudGroupEventTransformer("cloud_group", "CREATE")

    rows = transformer.transform_raw_event(
        {
            "id": "group.OCI.group-1",
            "add": {"identities": []},
            "remove": {"identities": []},
        }
    )

    assert len(rows) == 1
    assert rows[0]["id"] == "group.OCI.group-1"
    assert rows[0]["identity_operation_type"] == ""
