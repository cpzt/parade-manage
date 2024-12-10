
import heapq
from typing import List

from parade_manage.common.dag import DAG
from parade_manage.common.node import Node


def topological_sort(dag: DAG) -> List[Node]:
    queue = []
    for node_id, degree in dag.in_degree.items():
        if degree == 0:
            node = dag.get_node(node_id)
            heapq.heappush(queue, (node.priority * -1, node))  # max heap

    sorted_nodes = []
    in_degree = dag.in_degree.copy()

    while queue:
        priority, node = heapq.heappop(queue)
        sorted_nodes.append(node)

        succ_nodes = dag.successor(node)
        for node in succ_nodes:
            node_id = node.node_id
            in_degree[node_id] -= 1
            if in_degree[node_id] == 0:
                heapq.heappush(queue, (node.priority * -1, node))

    if len(sorted_nodes) != len(dag.graph):
        raise ValueError("Graph contains a cycle")

    return sorted_nodes
