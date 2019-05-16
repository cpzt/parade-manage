# -*- coding: utf-8 -*-

"""
parade manager for managing `parade`
"""
import re
import inspect

from contextlib import contextmanager
from collections import Iterable

from parade.core.task import Flow
from parade.core.context import Context
from parade.core.engine import Engine
from parade.utils.workspace import load_bootstrap


FLOW_PREFIX = 'flow_'


class ParadeManage:

    def __init__(self):
        self.linked_tasks = {}
        self._source = {}
        self.flows = {}
        self.init()

    def init(self):
        boot = load_bootstrap()
        self.context = Context(boot)
        self.tasks_obj = self.context.load_tasks()  # all task object
        self.tasks = list(self.tasks_obj.keys())  # all task name
        self.task_deps = {t.name: list(t.deps) for t in self.tasks_obj.values()} # task deps name
        self.reversed_tasks = self.reverse_task()
        self.task_flows = self.task_flow(self.reversed_tasks)

    def reverse_task(self):
        '''exchange deps and task'''
        res = {}
        for key, vals in self.task_deps.items():
            for val in vals:
                if val not in res:
                    res[val] = {}
                res[val][key] = {}
            if key not in res:
                res[key] = {}
        return res

    def task_flow(self, tasks):
        res = {}
        for key, vals in tasks.items():
            res[key] = {}
            if key in self.reversed_tasks:
                res[key] = self.task_flow(self.reversed_tasks[key])
        return res

    def links(self, key):
        children = self.task_flows[key]
        res = []
        self.to_link(children, res)
        res.insert(0, key)
        self.linked_tasks[key] = res

    def _get_task(self, key):
        if key not in self.task_flows:
            raise KeyError('{} not in data'.format(key))

        if key not in self.linked_tasks:
            self.links(key)

        tasks_link = self.linked_tasks[key]

        deps = dict()
        tasks = list()

        for task in tasks_link:
            deps_ = self.task_deps.get(task)
            tasks.append(task)
            deps[task] = [d for d in deps_ if d in tasks_link]
        self.flows[key] = (list(tasks), deps)

    def get_task(self, name):
        key = self._concat_names(name)
        if key not in self.flows:
            if isinstance(name, (str, int)):
                self._get_task(name)
            else:
                self._get_tasks(name)

        return self.flows[key]

    def _get_tasks(self, names):
        key_ = self._concat_names(names)
        tasks_all = set()
        deps_all = {}
        for name in names:
            if name not in self.flows:
                self._get_task(name)
            tasks, deps = self.flows[name]
            # print(tasks)
            tasks_all.update(tasks)
            for key, val in deps.items():
                if key not in deps_all:
                    deps_all[key] = val
                else:
                    val = set(deps_all[key] + val)
                    deps_all[key] = list(val)
        self.flows[key_] = (list(tasks_all), deps_all)

    def get_flow(self, names=None, flow_name=None):
        key = self._concat_names(names)
        if flow_name is None:
            flow_name = FLOW_PREFIX + self._concat_names(names) 
        tasks, deps = self.get_task(names)
        flow = Flow(flow_name, tasks, deps)

        self._flow = flow
        return flow

    def dump_flow(self, names=None, flow_name=None):
        flow = self.get_flow(names, flow_name)
        flow = flow.uniform()
        flow.dump()

    def run_flow(self, names, **kwargs):
        engine = Engine(self.context)
        
        flow_name = kwargs.get('flow_name')
        if not flow_name:
            flow_name = FLOW_PREFIX + self._concat_names(names)
        force = kwargs.get('force')
        return engine.execute_async(flow_name=flow_name, force=force)

    def rm_flow(self, names=None, flow_name=None):
        if flow_name is None:
            key = self._concat_names(names)
            flow_name = FLOW_PREFIX + key
        flowstore = self.context.get_flowstore()
        flowstore.delete(flow_name)

    def store_flow(self, names, flow_name=None):
        key = self._concat_names(names)
        if flow_name is None:
            flow_name = FLOW_PREFIX + key

        tasks, deps = self.get_task(names)
        
        # drop
        tasks = [t for t in tasks if t != flow_name]
        deps = {k:v for k, v in deps.items() if v}

        flowstore = self.context.get_flowstore()
        flowstore.create(flow_name, *tasks, deps=deps)

        print('Flow {} created, details:'.format(key))
        flow = flowstore.load(flow_name).uniform()
        print('tasks [{}]: {}'.format(len(flow.tasks), flow.tasks))
        print('dependencies:')
        print('------------------------------------------')
        flow.dump()
        print('------------------------------------------')

    def get_source(self, name):
        if not isinstance(name, str):
            raise TypeError('name except str, {} got'.format(type(name).__name__))
        task = self.context.get_task(name)
        lines = inspect.getsourcelines(task.__class__)
        source_code = self.drop_comments(lines)

        pattern = re.compile(r"get_stat\(\s*[\'\"](.*?)[\'\"]\s*,"
                                "|context.load\(\s*[\'\"](.*?)[\'\"]\s*,")
        items = pattern.findall(source_code)
        source = set(flatten(items))

        return list(source)

    @classmethod
    def to_link(cls, data, res):
        '''BFS'''
        for key in data.keys():
            if key and key not in res:
                res.append(key)
        t =  dict([(k, v) for val in data.values() for k, v in val.items()])
        if t:
            cls.to_link(t, res)

    @staticmethod
    def drop_comments(source):
        if isinstance(source, (tuple, list)):
            source = source[0]

        source = (s for s in source if not s.strip().startswith('#'))
        return '\n'.join(list(source))

    @staticmethod
    def _concat_names(names):
        if isinstance(names, (list, tuple)):
            names = sorted([str(n) for n in names])
            return '-'.join(names)
        elif isinstance(names, (str, int)):
            return str(names)
        else:
            raise TypeError('names expect int, str, list or tuple got {}'.format(
                            type(names).__name__))

    def __enter__(self):
        return self

    def __exit__(self, exc_ty, exc_val, exc_tb):
        pass

    def __repr__(self):
        return 'ParadeManager()'


def flatten(items, ignore_types=(bytes, str), ignore_flags=('', None)):
    for item in items:
        if item in ignore_flags:
            continue
        if isinstance(item, Iterable) and not isinstance(item, ignore_types):
            yield from flatten(item)
        else:
            yield item


