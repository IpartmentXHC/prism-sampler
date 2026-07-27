from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class GroupRule:
    name: str
    pattern: str


def group_name(comm: str, rules: list[GroupRule]) -> str:
    for rule in rules:
        if re.search(rule.pattern, comm):
            return rule.name
    return comm
