from copy import deepcopy


class DAG:
    def __init__(self):
        self._nodes = dict()
        self._graph = dict()
        self._reversed_graph = dict()

    @property
    def nodes(self):
        return [self._nodes[nid] for nid in self._graph]

    @property
    def graph(self):
        return self._graph

    @property
    def reversed_graph(self):
        return self._reversed_graph

    def add_node(self, node):
        node_id = id(node)
        self._nodes[node_id] = node
        self._graph[node_id] = set()
        self._reversed_graph[node_id] = set()

    def remove_node(self, node):
        node_id = id(node)
        del self._nodes[node_id]

        del self._graph[node_id]
        for node_id_set in self._graph.values():
            if node_id in node_id_set:
                node_id_set.remove(node_id)

        del self._reversed_graph[node_id]
        for dep_node_id_set in self._reversed_graph.values():
            if node_id in dep_node_id_set:
                dep_node_id_set.remove(node_id)

    def contains_node(self, node):
        return id(node) in self._nodes

    def add_edge(self, node, dep_node):
        nid, did = id(node), id(dep_node)
        if nid not in self._nodes or did not in self._nodes:
            raise KeyError('node does not exist')

        self._graph[did].add(nid)
        self._reversed_graph[nid].add(did)

    def remove_edge(self, node, dep_node):
        nid, did = id(node), id(dep_node)
        if nid not in self._nodes or did not in self._nodes:
            raise KeyError('node does not exist')

        if nid in self._graph[did]:
            self._graph[did].remove(nid)
            self._reversed_graph[nid].remove(did)

    def contains_edge(self, node, dep_node):
        nid, did = id(node), id(dep_node)
        if nid not in self._nodes or did not in self._nodes:
            return False

        return nid in self._graph[did]

    def find_no_dep_ids(self, reversed_graph=None):

        reversed_graph = reversed_graph or self._reversed_graph

        no_dep_ids = []
        for node_id, dep_node_id_set in reversed_graph.items():
            if len(dep_node_id_set) == 0:
                no_dep_ids.append(node_id)

        return no_dep_ids

    def topological_sort(self, graph=None, reversed_graph=None):
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

            child_node_ids = graph.pop(nid)
            for node_id in child_node_ids:
                reversed_graph[node_id].remove(nid)

                if len(reversed_graph[node_id]) == 0:
                    queue.append(node_id)

        if len(traversed_ids) != len(self._nodes):
            raise ValueError('Graph is not acyclic')

        return [self._nodes[nid] for nid in traversed_ids]

    def bfs(self, start_nodes=None):
        if start_nodes:
            start_nodes_ids = [id(node) for node in start_nodes]
        else:
            start_nodes_ids = self.find_no_dep_ids()

        assert all(nid in self._nodes for nid in start_nodes_ids)

        visited = set(start_nodes_ids)

        queue = [self._nodes[nid] for nid in start_nodes_ids]

        while len(queue) > 0:
            cur_node = queue.pop(0)
            yield cur_node

            for node in self.children(cur_node):
                if id(node) not in visited:
                    visited.add(id(node))
                    queue.append(node)

    def children(self, node):
        child_node_ids = self._graph[node]
        return [self._nodes[nid] for nid in child_node_ids]