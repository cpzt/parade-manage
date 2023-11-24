from __future__ import annotations
from copy import deepcopy
from typing import Dict, Set, List, Callable
from collections import defaultdict

from parade_manage.common.node import Node, NodeId


class DAG:
    def __init__(self):
        self._nodes: Dict[NodeId, Node] = dict()
        self._graph: Dict[Node, Set[Node]] = defaultdict(set)
        self._reversed_graph: Dict[Node, Set[Node]] = defaultdict(set)

    @property
    def node_map(self) -> Dict[NodeId, Node]:
        return self._nodes

    @property
    def nodes(self) -> List[Node]:
        return list(self._nodes.values())

    @property
    def graph(self) -> Dict[Node, Set[Node]]:
        return self._graph

    @property
    def reversed_graph(self) -> Dict[Node, Set[Node]]:
        return self._reversed_graph

    def add_node(self, node: Node):
        node_id = node.node_id

        if node_id not in self._nodes:
            self._nodes[node_id] = node
            self._graph[node] = set()
            self._reversed_graph[node] = set()

    def remove_node(self, node: Node):
        node_id = node.node_id
        del self._nodes[node_id]

        del self._graph[node]
        for node_id_set in self._graph.values():
            if node_id in node_id_set:
                node_id_set.remove(node)

        del self._reversed_graph[node]
        for dep_node_id_set in self._reversed_graph.values():
            if node_id in dep_node_id_set:
                dep_node_id_set.remove(node)

    def contains_node(self, node: Node) -> bool:
        return node.node_id in self._nodes

    def add_edge(self, node: Node, dep_node: Node):
        nid, did = node.node_id, dep_node.node_id
        if nid not in self._nodes or did not in self._nodes:
            raise KeyError('node does not exist')

        self._graph[dep_node].add(node)
        self._reversed_graph[node].add(dep_node)

    def remove_edge(self, node: Node, dep_node: Node):
        nid, did = node.node_id, dep_node.node_id
        if nid not in self._nodes or did not in self._nodes:
            raise KeyError('node does not exist')

        if nid in self._graph[dep_node]:
            self._graph[dep_node].remove(node)
            self._reversed_graph[node].remove(dep_node)

    def contains_edge(self, node: Node, dep_node: Node) -> bool:
        nid, did = node.node_id, dep_node.node_id
        if nid not in self._nodes or did not in self._nodes:
            return False

        return node in self._graph[dep_node]

    def find_no_dep_ids(self, reversed_graph: Dict[Node, Set[Node]] = None) -> List[NodeId]:

        reversed_graph = reversed_graph or self._reversed_graph

        no_dep_ids = []
        for node, dep_node_set in reversed_graph.items():
            if len(dep_node_set) == 0:
                no_dep_ids.append(node.node_id)

        return no_dep_ids

    def topological_sort(self, graph: Dict[Node, Set[Node]] = None, reversed_graph: Dict[Node, Set[Node]] = None
                         ) -> List[Node]:
        no_dep_ids = self.find_no_dep_ids()

        graph = graph or self._graph
        graph = deepcopy(graph)

        reversed_graph = reversed_graph or self._reversed_graph
        reversed_graph = deepcopy(reversed_graph)

        queue = list(no_dep_ids)

        traversed_ids = set()

        while len(queue) > 0:
            nid = queue.pop(0)
            traversed_ids.add(nid)

            dnode = self._nodes[nid]

            child_node_ids = graph.pop(nid)
            for node_id in child_node_ids:
                node = self._nodes[node_id]

                reversed_graph[node].remove(dnode)

                if len(reversed_graph[node]) == 0:
                    queue.append(node_id)

        if len(traversed_ids) != len(self._nodes):
            raise ValueError('Graph is not acyclic')

        return [self._nodes[nid] for nid in traversed_ids]

    def bfs(self, start_nodes: List[Node] = None):
        if start_nodes:
            start_nodes_ids = [node.node_id for node in start_nodes]
        else:
            start_nodes_ids = self.find_no_dep_ids()

        assert all(nid in self._nodes for nid in start_nodes_ids)

        visited = set(start_nodes_ids)

        queue = [self._nodes[nid] for nid in start_nodes_ids]

        while len(queue) > 0:
            cur_node = queue.pop(0)
            yield cur_node

            for node in self.children(cur_node):
                if node.node_id not in visited:
                    visited.add(node.node_id)
                    queue.append(node)

    def children(self, node: Node) -> List[Node]:
        return list(self._graph[node])

    def predecessor(self, node: Node) -> List[Node]:
        return list(self.reversed_graph[node])

    def successor(self, node: Node) -> List[Node]:
        return list(self.graph[node])

    def _traverse(self, nodes: List[Node], apply: Callable) -> Dict[Node, List[Node]]:
        all_deps = {}

        queue = nodes

        while len(queue) > 0:
            node = queue.pop()

            predecessor_nodes = apply(node)

            all_deps[node] = predecessor_nodes
            queue.extend(predecessor_nodes)

        return all_deps

    def all_predecessor(self, nodes: List[Node]) -> Dict[Node, List[Node]]:
        """
        get all predecessor node
        """
        return self._traverse(nodes, apply=self.predecessor)

    def all_successor(self, nodes: List[Node]) -> Dict[Node, List[Node]]:
        """
        get all successor node
        """
        return self._traverse(nodes, apply=self.successor)

    @property
    def isolated_nodes(self) -> List[Node]:
        """
        no predecessor and no successor
        :return: node
        """
        no_pred_nodes = [node for node, pnodes in self.reversed_graph.items() if len(pnodes) == 0]
        no_suc_nodes = [node for node, snodes in self.graph.items() if len(snodes) == 0]

        isolated_node = set(no_pred_nodes) & set(no_suc_nodes)

        return list(isolated_node)

    @property
    def root_nodes(self) -> List[Node]:
        no_pred_nodes = {node for node, pnodes in self.reversed_graph.items() if len(pnodes) == 0}
        return list(no_pred_nodes)

    @property
    def leaf_nodes(self) -> List[Node]:
        no_suc_nodes = {node for node, snodes in self.graph.items() if len(snodes) == 0}
        return list(no_suc_nodes)

    @classmethod
    def from_reversed_graph(cls, reversed_graph: Dict[Node, Set[Node] | List[Node]]) -> DAG:
        dag = DAG()
        for node, deps_nodes in reversed_graph.items():
            dag.add_node(node)
            for deps_node in deps_nodes:
                dag.add_node(deps_node)
                dag.add_edge(node, deps_node)

        return dag

    @classmethod
    def from_graph(cls, graph: Dict[Node, Set[Node] | List[Node]]) -> DAG:
        dag = DAG()
        for node, suc_nodes in graph.items():
            dag.add_node(node)
            for suc_node in suc_nodes:
                dag.add_node(suc_node)
                dag.add_edge(suc_node, node)

        return dag
