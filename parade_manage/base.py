# -*- coding: utf-8 -*-

"""
parade manager for managing `parade`
"""
from __future__ import annotations

import os
import sys
import yaml

from typing import List, Type, Dict
from datetime import datetime

from parade.core.task import Task
from parade.utils.modutils import iter_classes

from .utils import iter_classes, tree
from .common.dag import DAG


class ParadeManage:

    def __init__(self, project_path: str = None):

        self.project_path = self.init_context(project_path)

        self.dag = self.init_dag()

    @property
    def task_map(self):
        """
        return task-name -> task
        """
        return {node.name: node for node in self.dag.nodes}

    @property
    def project(self):
        """
        :return: current project name
        """
        return self._get_project_name()

    def __repr__(self):
        return '<ParadeManager(project_path={})>'.format(self.project_path)

    def init_context(self, project_path: str = None) -> str:
        """
        init project context
        :param project_path: target project path
        :return: project path
        """
        project_path = os.path.expanduser(project_path) if project_path is not None else os.getcwd()
        os.chdir(project_path)  # change project root path
        sys.path.insert(0, os.getcwd())

        return project_path

    def init_dag(self) -> DAG:

        project_name = self.project
        task_classes = iter_classes(Task, project_name + ".task")

        name_to_instance = self.init_task_classes(task_classes)
        reversed_graph = {task_instance: list(name_to_instance[deps_name] for deps_name in task_instance.deps)
                          for task_instance in name_to_instance.values()}

        return self.to_dag(reversed_graph)

    def init_task_classes(self, task_classes: List[Type]) -> Dict[str, Task]:
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
    def to_dag(cls, reversed_graph):
        dag = DAG.from_reversed_graph(reversed_graph)
        return dag

    def dump(self, target_tasks: str | List = None, flow_name: str = None):
        """
        dump and generate file
        :param target_tasks: target tasks or None
        :param flow_name: flow name or None
        """
        flow_name = flow_name or "flow-" + datetime.now().strftime("%Y%m%d")

        if target_tasks is None:
            tasks = self.dag.nodes
        else:
            if isinstance(target_tasks, str):
                target_tasks = [target_tasks]

            target_task_instances = [self.task_map[task] for task in target_tasks]
            tasks = self.dag.all_predecessor(target_task_instances)

        task_names = [task.name for task in tasks]
        deps = ["{task_name}->{task_deps}".format(task_name=task.name, task_deps=",".join(task.deps))
                for task in tasks if len(task.deps) > 0]

        data = {"tasks": task_names, "deps": deps}

        class IndentDumper(yaml.Dumper):
            def increase_indent(self, flow=False, indentless=False):
                return super(IndentDumper, self).increase_indent(flow, False)

        with open("./flows/" + flow_name + ".yml", "w") as f:
            yaml.dump(data, f, Dumper=IndentDumper, default_flow_style=False)

    def tree(self, name, task_names: List = None):
        """
        show task
        :param name: name of flow
        :param task_names: task names or None
        """
        if task_names is None or len(task_names) == 0:
            nodes = self.dag.nodes
        else:
            nodes = self.dag.all_successor([self.task_map[task_name] for task_name in task_names])

        task_map = dict()
        for node in nodes:
            node_id = id(node)
            task = self.dag.node_map[node_id]
            children = list(task.deps)
            task_map[task.name] = children

        task_map[name] = list(task_map.keys())

        tree(task_map, name)

