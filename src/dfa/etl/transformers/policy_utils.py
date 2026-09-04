# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import re
from typing import Any, Dict, List, Tuple

ALLOWED_BASE_VERBS = {"manage", "use", "read", "inspect"}
CROSS_TENANCY_VERBS = {"admit", "endorse"}  # treat as valid org/cross-tenancy constructs

# Baseline scores from the policy review rubric.  Keeping this as a matrix makes
# the non-linear parts of the rubric explicit (for example, ``use all-resources``
# in a compartment is less permissive than the same grant across a tenancy).
BASE_SCORES = {
    "manage": {
        "specific": {"compartment": 3, "tenancy": 4, "unknown": 3},
        "family": {"compartment": 4, "tenancy": 4, "unknown": 4},
        "all": {"compartment": 4, "tenancy": 5, "unknown": 4},
    },
    "use": {
        "specific": {"compartment": 2, "tenancy": 2, "unknown": 2},
        "family": {"compartment": 3, "tenancy": 3, "unknown": 3},
        "all": {"compartment": 2, "tenancy": 4, "unknown": 2},
    },
    "read": {
        "specific": {"compartment": 2, "tenancy": 2, "unknown": 2},
        "family": {"compartment": 3, "tenancy": 3, "unknown": 3},
        "all": {"compartment": 2, "tenancy": 4, "unknown": 2},
    },
    "inspect": {
        "specific": {"compartment": 2, "tenancy": 2, "unknown": 2},
        "family": {"compartment": 3, "tenancy": 3, "unknown": 3},
        "all": {"compartment": 2, "tenancy": 4, "unknown": 2},
    },
    "service-defined-actions": {
        "specific": {"compartment": 3, "tenancy": 4, "unknown": 3},
    },
    "unknown": {
        "specific": {"compartment": 3, "tenancy": 3, "unknown": 3},
        "family": {"compartment": 3, "tenancy": 3, "unknown": 3},
        "all": {"compartment": 3, "tenancy": 3, "unknown": 3},
    },
}


def split_once_regex(s: str, pattern: str) -> Tuple[str, str]:
    """Split s on the first occurrence of regex pattern (case-insensitive). Returns (before, after or '')."""
    m = re.search(pattern, s, flags=re.IGNORECASE)
    if not m:
        return s, ""
    return s[: m.start()].strip(), s[m.end() :].strip()


def normalize_ws(s: str) -> str:
    return " ".join(s.strip().split())


def _condition_risk(conditions: str) -> Tuple[bool, bool]:
    """Return whether a condition is broad and/or restrictive.

    A condition may contain both traits.  An ``any`` clause or a negative
    predicate broadens the match, while a positive equality/membership test
    narrows it.  Applying both adjustments is important: a mixed condition
    should not gain risk merely because it also contains a real restriction.
    """
    cond_l = conditions.lower()
    if not cond_l:
        return False, False

    broad = bool(re.search(r"!\s*=|\bnot\s+in\b|\bany\s*\{|=\s*/\s*\*|\bregex\b", cond_l))

    # Normalize spaced negative operators before looking for positive equals.
    normalized = re.sub(r"!\s*=", "!=", cond_l)
    positive_equality = False
    for match in re.finditer(r"=", normalized):
        before = normalized[: match.start()].rstrip()
        after = normalized[match.end() :].lstrip()
        if before.endswith(("!", "<", ">", "=")) or after.startswith("="):
            continue
        # A wildcard followed only by a suffix (for example ``/*-stg/``)
        # does not materially narrow the grant. Other wildcard patterns still
        # select a meaningful resource name or namespace.
        if after.startswith("/*-"):
            continue
        if before.endswith("request.instanceoptions.arelegacyendpointsdisabled"):
            continue
        positive_equality = True
        break

    positive_membership = any(
        not normalized[: match.start()].rstrip().endswith("not") for match in re.finditer(r"\bin\s*\(", normalized)
    )
    return broad, positive_equality or positive_membership


def _base_score(verb: str, resource_breadth: str, scope_kind: str) -> int:
    verb_scores = BASE_SCORES.get(verb, BASE_SCORES["unknown"])
    breadth_scores = verb_scores.get(resource_breadth, verb_scores["specific"])
    return breadth_scores.get(scope_kind, breadth_scores["unknown"])


def _is_unrestricted_service_grant(
    subject_type: str,
    subject_raw: str,
    verb: str,
    scope_kind: str,
    conditions: str,
) -> bool:
    """Identify unconditioned tenancy-wide grants to service-named groups."""
    if subject_type != "group" or verb != "manage" or scope_kind != "tenancy" or conditions:
        return False

    group_name = re.sub(r"^group\s+", "", subject_raw, flags=re.IGNORECASE)
    return bool(re.search(r"(?:^|[-_])service(?:[-_](?:group|managers))?$", group_name, flags=re.IGNORECASE))


# pylint: disable=too-many-return-statements
def detect_subject_type(subject_raw: str) -> str:
    s = subject_raw.strip().lower()
    if s.startswith("any-user") or " any-user" in s:
        return "any-user"
    if s.startswith("any-group") or " any-group" in s:
        return "any-group"
    if s.startswith("group") or s.startswith("'group") or s.startswith('"group') or " group " in s:
        return "group"
    if s.startswith("dynamic-group") or " dynamic-group " in s:
        return "dynamic-group"
    if s.startswith("service"):
        return "service"
    if s.startswith("resource"):
        return "resource-principal"
    return "unknown"


# pylint: disable=too-many-locals
def parse_policy_statement(line: str) -> Dict[str, Any]:
    original = line.rstrip("\n")
    norm = normalize_ws(original)
    if not norm:
        return {}

    # relation (allow/admit/endorse)
    m = re.match(r"^(allow|admit|endorse)\s+(.*?)\s+to\s+(.+)$", norm, flags=re.IGNORECASE)
    if not m:
        # could be malformed; skip
        return {"parsed": False, "original": original, "error": "Unrecognized statement format"}

    relation = m.group(1).lower()
    subject_raw = m.group(2).strip()
    tail = m.group(3).strip()

    # action segment and rest (scope/conditions)
    action_segment, after_in = split_once_regex(tail, r"\s+in\s+")

    actions_in_braces: List[str] = []
    primary_verb = ""
    resource_phrase = ""

    if action_segment.strip().startswith("{"):
        # service-defined actions
        end_brace = action_segment.find("}")
        inside = action_segment[1:end_brace] if end_brace != -1 else action_segment[1:]
        # split by comma or whitespace
        actions_in_braces = [a for a in re.split(r"[\s,]+", inside) if a]
        primary_verb = "service-defined-actions"
        resource_phrase = ""
    else:
        # try standard verbs
        m2 = re.match(r"^(manage|use|read|inspect)\s+(.*)$", action_segment, flags=re.IGNORECASE)
        if m2:
            primary_verb = m2.group(1).lower()
            resource_phrase = m2.group(2).strip()
        else:
            primary_verb = "unknown"
            resource_phrase = action_segment.strip()

    # scope and conditions
    scope_phrase = ""
    conditions = ""
    if after_in:
        scope_phrase, conditions = split_once_regex(after_in, r"\s+where\s+")

    subject_type = detect_subject_type(subject_raw)
    cross_tenancy = relation in CROSS_TENANCY_VERBS or bool(
        re.search(r"\bof\s+(any-)?tenancy\b", subject_raw, flags=re.IGNORECASE)
    )

    # Risk evaluation -> numeric score 1 (least permissive) to 5 (overly permissive)
    reasons: List[str] = []

    res_l = resource_phrase.lower()
    scope_l = scope_phrase.lower()
    # classify resource breadth
    resource_breadth = "specific"
    if "all-resources" in res_l:
        resource_breadth = "all"
    elif "-family" in res_l:
        resource_breadth = "family"

    # classify scope
    scope_kind = "unknown"
    if re.match(r"^compartment\b", scope_l):
        scope_kind = "compartment"
    elif re.match(r"^(any-)?tenancy\b", scope_l):
        scope_kind = "tenancy"

    # Base score from action/resource/scope per reviewed guideline.
    score = _base_score(primary_verb, resource_breadth, scope_kind)
    reasons.append(
        f"Base score {score} from action/resource/scope "
        f'(action={primary_verb}, resource="{resource_phrase}", scope="{scope_phrase}")'
    )

    # Cross-tenancy trust and condition adjustments are independent.
    if cross_tenancy:
        score = min(score + 1, 5)
        reasons.append(f"{relation.title()} cross-tenancy trust")

    cond_broad, cond_restrictive = _condition_risk(conditions)
    if cond_broad:
        score = min(score + 1, 5)
        reasons.append("Broad or negative match in conditions")
    if cond_restrictive:
        score = max(score - 1, 1)
        reasons.append("Restrictive conditions present")

    if (
        primary_verb == "manage"
        and resource_breadth in ("family", "all")
        and scope_kind == "tenancy"
        and not conditions
    ):
        score = 5
        reasons.append("Unconditioned family/all-resources management access in tenancy")

    # Subject floors and tenancy-wide principal amplifiers.
    if subject_type in ("any-user", "any-group"):
        any_tenancy = "any-tenancy" in subject_raw.lower() or scope_l.startswith("any-tenancy")
        if any_tenancy and primary_verb in ("manage", "service-defined-actions"):
            score = 5
            reasons.append("Any-user/group from any-tenancy")
        else:
            score = max(score, 4)
            reasons.append("Any-user subject")
    elif subject_type == "dynamic-group" and primary_verb == "manage" and scope_kind == "tenancy":
        score = 5
        reasons.append("Dynamic group with manage access in tenancy")

    # Service principal tenancy-wide manage
    if subject_type == "service" and primary_verb == "manage" and scope_kind == "tenancy":
        score = 5
        reasons.append("Service principal with manage access in tenancy")

    if _is_unrestricted_service_grant(
        subject_type,
        subject_raw,
        primary_verb,
        scope_kind,
        conditions,
    ):
        score = 5
        reasons.append("Unrestricted service management access in tenancy")

    # Unknown action phrase outside braces
    unknown_action = primary_verb == "unknown" and not actions_in_braces
    if unknown_action:
        reasons.append("Nonstandard action phrase (outside braces)")

    return {
        "parsed": True,
        "original": original,
        "relation": relation,
        "subject_raw": subject_raw,
        "subject_type": subject_type,
        "actions_in_braces": ";".join(actions_in_braces) if actions_in_braces else "",
        "primary_verb": primary_verb,
        "resource_phrase": resource_phrase,
        "scope_phrase": scope_phrase,
        "conditions": conditions,
        "cross_tenancy": "Yes" if cross_tenancy else "No",
        "permissive_score": str(score),
        "reasons": "; ".join(reasons) if reasons else "",
        "unknown_action": "Yes" if unknown_action else "No",
    }


def remediation_for(row: Dict[str, Any]) -> str:
    reasons = row.get("reasons", "")
    rel = row.get("relation", "")
    subj_type = row.get("subject_type", "")
    verb = row.get("primary_verb", "")
    resource = row.get("resource_phrase", "")

    recs: List[str] = []
    if subj_type in ("any-user", "any-group"):
        recs.append(
            "Replace any-user/any-group with specific group; "
            "add networkSource or tag conditions; scope to specific compartments"
        )
    if "manage all-resources" in reasons.lower() or (verb == "manage" and "all-resources" in resource.lower()):
        recs.append("Replace manage all-resources with least-privilege resource types; scope to compartments")
    if "Service principal manage all-resources" in reasons:
        recs.append("Constrain service principal to compartments and minimal verbs (read/inspect)")
    if "cross-tenancy" in reasons.lower() or rel in CROSS_TENANCY_VERBS:
        recs.append(
            "Review cross-tenancy trust; restrict actions/resources; enforce conditions; document business need"
        )
    if "Broad or negative match" in reasons:
        recs.append("Avoid wildcards/negative matches; enumerate explicit resources/operations")
    if row.get("unknown_action") == "Yes":
        recs.append(
            "Verify action against OCI policy reference; use standard verbs or service-defined actions in braces"
        )
    if not recs:
        # generic
        recs.append("Review for least privilege and proper scoping")
    return " | ".join(recs)
