from __future__ import annotations
from typing import Any, List, Collection

NodeId = int


class Node:

    def __init__(self, name: str, value: Any):
        self.name = name
        self.value = value

    @property
    def node_id(self) -> NodeId:
        return NodeId(hash(self.name))

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):
        return f"Node<name={self.name}>"

    def __eq__(self, other):
        return self.name == other.name

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
