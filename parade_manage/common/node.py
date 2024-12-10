from __future__ import annotations
from typing import Any, List, Collection

NodeId = int


class Node:

    def __init__(self, name: str, value: Any, priority: int = 0):
        self.name = name
        self.value = value
        # The priority of the node.
        # Higher values indicate higher priority (default is 0).
        self.priority = self._validate_priority(priority)

    def set_priority(self, priority: int) -> None:
        assert priority >= 0, "`priority` must be a non-negative integer"
        self.priority = self._validate_priority(priority)

    @staticmethod
    def _validate_priority(value: int) -> int:
        if not isinstance(value, int) or value < 0:
            raise ValueError("`priority` must be a non-negative integer")
        return value

    @property
    def node_id(self) -> NodeId:
        return NodeId(hash(self.name))

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):
        return f"Node(name={self.name}, value={self.value}, priority={self.priority})"

    def __eq__(self, other):
        return self.name == other.name

    def __lt__(self, other):
        return self.priority > other.priority

    @staticmethod
    def build_list(elements: Collection) -> List[Node]:
        if len(elements) == 0:
            return []

        if isinstance(elements, (list, set)):
            nodes = []

            for element in elements:
                if isinstance(elements[0], (list, tuple, set)) and len(elements[0]) >= 2:
                    node = Node(element[0], element[1])
                elif isinstance(elements[0], (list, tuple, set)) and len(elements[0]) == 1:
                    node = Node(element[0], element[0])
                else:
                    node = Node(element, element)
                nodes.append(node)
            return nodes
        else:
            raise ValueError(f"only support list or set. elements is {elements}.")
