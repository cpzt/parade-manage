
from unittest import TestCase

from parade_manage.common.dag import DAG

from parade_manage import ParadeManage
from parade_manage.utils import walk_modules, tree


class Test(TestCase):

    def setUp(self) -> None:
        self.tasks = {
            "g": [], "h": [], "d": ["g"],
            "e": ["g", "h"], "a": ["d", "e"],
            "b": ["e", "f"], "c": ["f"],
            "k": []
        }

    def test_to_dag(self):
        ParadeManage.to_dag({"a": ["p1", "p2"]})

    def test_walk_modules(self):
        print(walk_modules("../parade_manage/common"))

    def test_dump(self):
        pass

    def test_tree(self):

        tasks = {"flow-1": ["a", "b", "c"], "a": []}

        tree(tasks, "flow-1")

    def test_leaf_nodes(self):
        tasks = self.tasks
        dag = DAG.from_graph(tasks)
        self.assertEqual(dag.leaf_nodes, ["g", "h", "f"])

    def test_root_nodes(self):
        tasks = self.tasks
        dag = DAG.from_graph(tasks)
        self.assertEqual(dag.root_nodes, ["a", "b", "c"])

    def test_isolated_nodes(self):
        tasks = self.tasks
        dag = DAG.from_graph(tasks)
        self.assertEqual(dag.isolated_nodes, ["k"])
