# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import re
from typing import Any, Dict, List, Optional, Tuple

ALLOWED_BASE_VERBS = {"manage", "use", "read", "inspect"}
ORG_LEVEL_RELATIONS = {"admit", "endorse"}  # treat as valid org/cross-tenancy constructs


def split_once_regex(s: str, pattern: str) -> Tuple[str, str]:
    """Split s on the first occurrence of regex pattern (case-insensitive). Returns (before, after or '')."""
    m = re.search(pattern, s, flags=re.IGNORECASE)
    if not m:
        return s, ""
    return s[: m.start()].strip(), s[m.end() :].strip()


def normalize_ws(s: str) -> str:
    return " ".join(s.strip().split())


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
def parse_policy_statement(line: str) -> Optional[Dict[str, Any]]:
    original = line.rstrip("\n")
    norm = normalize_ws(original)
    if not norm:
        return None

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
    cross_tenancy = bool(re.search(r"\bof\s+(any-)?tenancy\b", subject_raw, flags=re.IGNORECASE))

    # Risk evaluation -> numeric score 1 (least permissive) to 5 (overly permissive)
    risk_reasons: List[str] = []

    res_l = resource_phrase.lower()
    scope_l = scope_phrase.lower()
    subj_l = subject_raw.lower()

    # classify resource breadth
    resource_breadth = 0
    if "all-resources" in res_l:
        resource_breadth = 2
    elif re.search(r"\b(iam|secret|key|log|virtual-network)-family\b", res_l):
        resource_breadth = 1

    # classify scope
    scope_kind = 0
    if re.search(r"\btenancy\b", scope_l):
        scope_kind = 2
    elif re.search(r"\bcompartment\b", scope_l):
        scope_kind = 1

    # classify action
    action = 0
    if primary_verb == "inspect":
        action = 0
    elif primary_verb == "read":
        action = 1
    elif primary_verb == "use":
        action = 2
    elif primary_verb == "manage":
        action = 3

    # base score from action/resource/scope per guideline
    score = min(5, action + resource_breadth + scope_kind)
    if score >= 4:
        risk_reasons.append(
            f"High score from action/resource/scope "
            f'(action={primary_verb},resource="{resource_phrase}", scope="{scope_phrase}")'
        )

    # Subject amplifiers
    if subject_type in ("any-user", "any-group"):
        if "any-tenancy" in subj_l:
            score = 5
            risk_reasons.append("Any-user/group of any-tenancy")
        elif primary_verb == "service-defined-actions":
            score = 5
            risk_reasons.append("Any-user/group with service-defined actions")
        else:
            score = max(score, 4)
            risk_reasons.append("Any-user subject")
    elif subject_type == "dynamic-group":
        if resource_breadth >= 2 or scope_kind >= 2:
            if primary_verb == "manage":
                score = 5
                risk_reasons.append("Dynamic group with broad resource and manage verb")
            elif primary_verb != "inspect":
                score = max(score, 4)
                risk_reasons.append("Dynamic group with broad resource and powerful verb")

    # Service principal tenancy-wide manage
    if subject_type == "service" and primary_verb == "manage" and scope_kind == "tenancy":
        score = 5
        risk_reasons.append("Service principal manage across tenancy")

    # Org-level admit/endorse adjustments
    if relation in ORG_LEVEL_RELATIONS:
        if subject_type == "any-user" or "any-tenancy" in subj_l:
            score = 5
            risk_reasons.append(f"{relation.title()} of any-user/any-tenancy")
        elif cross_tenancy:
            score = min(max(score, 3) + 1, 5)
            risk_reasons.append(f"{relation.title()} cross-tenancy trust")

    # Conditions effect: conditions breadth/restrictiveness
    cond_l = conditions.lower()
    cond_broad = any(tok in cond_l for tok in ["!=", " any {", " regex", "/*", "/**", "\\"])
    cond_restrictive = (
        bool(conditions)
        and not cond_broad
        and bool(re.search(r"\brequest.principal.id\b", conditions, flags=re.IGNORECASE))
    )
    if cond_broad:
        score = min(score + 1, 5)
        risk_reasons.append("Broad or negative match in conditions")
    elif cond_restrictive:
        score = max(score - 1, 1)
        risk_reasons.append("Restrictive conditions present")

    # Unknown action phrase outside braces
    unknown_action = primary_verb == "unknown" and not actions_in_braces
    if unknown_action and scope_kind == "tenancy":
        score = max(score, 4)
    if unknown_action:
        risk_reasons.append("Nonstandard action phrase (outside braces)")

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
        "risk_score": str(score),
        "risk_reasons": "; ".join(risk_reasons) if risk_reasons else "",
        "unknown_action": "Yes" if unknown_action else "No",
    }


def remediation_for(row: Dict[str, Any]) -> str:
    reasons = row.get("risk_reasons", "")
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
    if "manage all-resources" in reasons.lower() or (
        verb == "manage" and "all-resources" in resource.lower()
    ):
        recs.append(
            "Replace manage all-resources with least-privilege resource types; scope to compartments"
        )
    if "Service principal manage all-resources" in reasons:
        recs.append("Constrain service principal to compartments and minimal verbs (read/inspect)")
    if "cross-tenancy" in reasons.lower() or rel in ORG_LEVEL_RELATIONS:
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
