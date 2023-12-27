# -*- coding: utf-8 -*-

"""
parade manager for managing `parade`
"""
from __future__ import annotations

import os
import sys

import yaml
import prettytable as pt

from typing import List, Type, Dict
from datetime import datetime

from parade.core.task import Task as ParadeTask
from parade.utils.modutils import iter_classes
from parade_manage.common.node import Node

from .utils import iter_classes, tree, show_check_info
from .common.dag import DAG


class ParadeManage:

    def __init__(self, project_path: str = None, env: str = None):
        self.env = env
        self.project_path = self.init_context(project_path, env)

        self.dag: DAG = self.init_dag()

    @property
    def task_map(self) -> Dict[str, Node]:
        """
        return task-name -> task
        """
        return {node.name: node for node in self.dag.nodes}

    @property
    def project(self) -> str:
        """
        :return: current project name
        """
        return self._get_project_name()

    def __repr__(self) -> str:
        return '<ParadeManager(project_path={}, env={})>'.format(self.project_path, self.env)

    def init_context(self, project_path: str = None, env: str = None) -> str:
        """
        init project context
        :param env: environment of project
        :param project_path: target project path
        :return: current project path
        """
        project_path = os.path.expanduser(project_path) if project_path is not None else os.getcwd()
        os.chdir(project_path)  # change project root path
        sys.path.insert(0, os.getcwd())

        if env:
            os.environ["PARADE_PROFILE"] = env

        return project_path

    def init_dag(self) -> DAG:

        project_name = self.project
        task_classes = iter_classes(ParadeTask, project_name + ".task")

        name_to_instance = self.init_task_classes(task_classes)

        reversed_graph: Dict[ParadeTask, List[ParadeTask]] = dict()

        for task_instance in name_to_instance.values():
            reversed_graph[task_instance] = []

            for deps_name in task_instance.deps:
                deps_task = name_to_instance[deps_name]
                reversed_graph[task_instance].append(deps_task)

        return self.to_dag(reversed_graph)

    def init_task_classes(self, task_classes: List[Type]) -> Dict[str, ParadeTask]:
        name_to_instance = {}
        for task_class in task_classes:
            task_instance = task_class()
            name_to_instance[task_instance.name] = task_instance

        return name_to_instance

    def _get_project_name(self) -> str:
        with open("parade.bootstrap.yml", "r") as f:
            conf = yaml.load(f, Loader=yaml.FullLoader)

        return conf['config']['name']

    @classmethod
    def to_dag(cls, reversed_graph: Dict[ParadeTask, List[ParadeTask]]) -> DAG:
        node_reversed_graph = {Node(k.name, k): [Node(v.name, v) for v in vs] for k, vs in reversed_graph.items()}
        dag = DAG.from_reversed_graph(node_reversed_graph)
        return dag

    def dump(self, target_tasks: str | List[str] = None, flow_name: str = None):
        """
        dump and generate file
        :param target_tasks: target tasks or None
        :param flow_name: flow name or None
        """
        flow_name = flow_name or "flow-" + datetime.now().strftime("%Y%m%d")

        if target_tasks is None:
            nodes = self.dag.nodes
        else:
            if isinstance(target_tasks, str):
                target_tasks = [target_tasks]

            current_nodes = [self.task_map[task] for task in target_tasks]
            nodes = self.dag.all_predecessor(current_nodes)

        tasks = [node.value for node in nodes]
        task_names = [task.name for task in tasks]
        deps = ["{task_name}->{task_deps}".format(task_name=task.name, task_deps=",".join(task.deps))
                for task in tasks if len(task.deps) > 0]

        data = {"tasks": task_names, "deps": deps}

        class IndentDumper(yaml.Dumper):
            def increase_indent(self, flow=False, indentless=False):
                return super(IndentDumper, self).increase_indent(flow, False)

        with open("./flows/" + flow_name + ".yml", "w") as f:
            yaml.dump(data, f, Dumper=IndentDumper, default_flow_style=False)

    def dump_with_prefix(self, prefix: str, flow_name: str = None):
        target_tasks = [task_name for task_name in self.task_map.keys() if task_name.startswith(prefix)]

        assert len(target_tasks) > 0, f"does not find task with prefix `{prefix}`"
        self.dump(target_tasks, flow_name)

    def tree(self, flow_name: str, task_names: List = None):
        """
        show task
        :param flow_name: name of flow
        :param task_names: task names or None
        """
        if task_names is None or len(task_names) == 0:
            nodes = self.dag.nodes
        else:
            nodes = self.dag.all_successor([self.task_map[task_name] for task_name in task_names])

        task_map: Dict[str, List[str]] = dict()
        for node in nodes:
            task: ParadeTask = node.value
            children = list(task.deps)
            task_map[task.name] = children

        task_map[flow_name] = list(task_map.keys())

        tree(task_map, flow_name)

    def show(self, task_names: List[str] = None, keyword: str = None):
        """show task in table"""
        if task_names is None or len(task_names) == 0:
            nodes = self.dag.nodes
        else:
            nodes = self.dag.all_successor([self.task_map[task_name] for task_name in task_names])

        tb = pt.PrettyTable()

        tb.field_names = ["name", "deps", "description"]

        for node in nodes:
            task = node.value
            description = (getattr(task, "description", "") or getattr(task, "describe", "") or
                           getattr(task, "__doc__", "")) or ""
            if keyword:
                if self._filter_item([task.name, description], keyword):
                    tb.add_row([task.name, "\n".join(task.deps), description.strip()], divider=True)
            else:
                tb.add_row([task.name, "\n".join(task.deps), description.strip()], divider=True)

        print(f"Total: {len(tb.rows)}")
        print(tb)

    def _filter_item(self, items: List[str], keyword: str) -> bool:
        """filter item"""
        return len([item for item in items if keyword is not None and keyword in item]) > 0

    @property
    def isolated_tasks(self) -> List[str]:
        """
        no predecessor and no successor
        :return: task name
        """
        return [node.name for node in self.dag.isolated_nodes]

    @property
    def leaf_tasks(self) -> List[str]:
        """
        :return: task name
        """
        return [node.name for node in self.dag.leaf_nodes]

    @property
    def root_tasks(self) -> List[str]:
        """
        :return: task name
        """
        return [node.name for node in self.dag.root_nodes]

    def check(self):
        reversed_graph = self.dag.reversed_graph
        tasks = {t.name: [n.name for n in d] for t, d in reversed_graph.items()}

        show_check_info(tasks)
