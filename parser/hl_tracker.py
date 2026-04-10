"""
HL (Hierarchical Level) tracker for 837P loop hierarchy.

HL segment format:  HL*id*parent_id*level_code*child_flag~

Level codes (005010X222A1):
  20 → Information Source  (Billing Provider)  → loop 2000A
  22 → Subscriber                               → loop 2000B
  23 → Dependent (Patient)                      → loop 2000C

Parent-child rules:
  20  must have no parent  (or parent "" / "0")
  22  must have parent of type 20
  23  must have parent of type 22
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)

HL_LOOP_MAP = {
    "20": "2000A",
    "22": "2000B",
    "23": "2000C",
}

VALID_PARENTS: dict[str, set[str]] = {
    "20": {""},          # billing provider has no parent
    "22": {"20"},        # subscriber's parent must be billing provider
    "23": {"22"},        # dependent's parent must be subscriber
}


@dataclass
class HLNode:
    hl_id: str
    parent_id: str
    level_code: str
    child_flag: str
    loop: str
    position: int


@dataclass
class HLError:
    message: str
    segment: str
    position: int
    level_code: str
    hl_id: str


class HLTracker:
    """
    Maintains the HL hierarchy for a single ST-SE transaction.
    Call :meth:`process` for each HL segment in order.
    Errors accumulate in :attr:`errors`.
    """

    def __init__(self) -> None:
        self._nodes: dict[str, HLNode] = {}
        self.errors: list[HLError] = []
        self._current: Optional[HLNode] = None

    def process(self, segment: str, ed: str, position: int) -> HLNode:
        """
        Parse an HL segment and validate parent-child relationships.

        Returns the HLNode for this HL, even when errors are detected,
        so that parsing can continue.
        """
        els = segment.split(ed)
        hl_id = els[1] if len(els) > 1 else ""
        parent_id = els[2] if len(els) > 2 else ""
        level_code = els[3] if len(els) > 3 else ""
        child_flag = els[4] if len(els) > 4 else "0"

        loop = HL_LOOP_MAP.get(level_code, f"2000_{level_code}")

        node = HLNode(
            hl_id=hl_id,
            parent_id=parent_id,
            level_code=level_code,
            child_flag=child_flag,
            loop=loop,
            position=position,
        )

        self._validate(node, segment, position)
        self._nodes[hl_id] = node
        self._current = node
        return node

    def _validate(self, node: HLNode, segment: str, position: int) -> None:
        valid_parent_codes = VALID_PARENTS.get(node.level_code)

        if valid_parent_codes is None:
            # Unknown level code — warn but don't block
            log.warning("Unknown HL level code %r at position %d.", node.level_code, position)
            return

        if node.parent_id == "":
            # No parent declared
            if "" not in valid_parent_codes:
                self.errors.append(HLError(
                    message=(
                        f"HL id={node.hl_id} level={node.level_code} has no parent "
                        f"but expected parent of type {valid_parent_codes}."
                    ),
                    segment=segment,
                    position=position,
                    level_code=node.level_code,
                    hl_id=node.hl_id,
                ))
            return

        # Parent must exist in the nodes seen so far
        parent_node = self._nodes.get(node.parent_id)
        if parent_node is None:
            self.errors.append(HLError(
                message=(
                    f"HL id={node.hl_id} level={node.level_code} references "
                    f"parent_id={node.parent_id!r} which has not been seen."
                ),
                segment=segment,
                position=position,
                level_code=node.level_code,
                hl_id=node.hl_id,
            ))
            return

        if parent_node.level_code not in valid_parent_codes:
            self.errors.append(HLError(
                message=(
                    f"HL id={node.hl_id} level={node.level_code} has parent "
                    f"level={parent_node.level_code!r}, expected one of {valid_parent_codes}."
                ),
                segment=segment,
                position=position,
                level_code=node.level_code,
                hl_id=node.hl_id,
            ))

    @property
    def current(self) -> Optional[HLNode]:
        return self._current

    def get(self, hl_id: str) -> Optional[HLNode]:
        return self._nodes.get(hl_id)

    def reset(self) -> None:
        self._nodes.clear()
        self.errors.clear()
        self._current = None
