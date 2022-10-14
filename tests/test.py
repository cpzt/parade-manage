
from unittest import TestCase

from src.parade_manage import ParadeManage
from src.utils import walk_modules, tree


class Test(TestCase):

    def test_to_dag(self):
        ParadeManage.to_dag({"a": ["p1", "p2"]})

    def test_walk_modules(self):
        print(walk_modules("../src/common"))

    def test_dump(self):
        pass

    def test_tree(self):

        tasks = {"flow-1": ["a", "b", "c"], "a": []}

        tree(tasks, "flow-1")

