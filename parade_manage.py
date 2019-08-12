# -*- coding: utf-8 -*-

"""
parade manager for managing `parade`
"""
import os
import re
import inspect

from contextlib import contextmanager
from collections import Iterable
from queue import Queue

from parade.core.task import Flow
from parade.core.context import Context
from parade.core.engine import Engine
from parade.utils.workspace import load_bootstrap


FLOW_PREFIX = 'flow_'
SOURCE_FLOW_PREFIX = 'source_flow_'
SOURCE_PREFIX = 'source_'


class ParadeManage:

    def __init__(self):
        self.linked_tasks = {}
        self.linked_source = {}
        self.task_flows = {}
        self.source_flows = {}
        self.source_deps = {}
        self._source_pattern = None
        self.pattern = None
        self.init()

    def init(self):
        boot = load_bootstrap()
        self.context = Context(boot)
        self.tasks_obj = self.context.load_tasks()  # all task object
        self.tasks = list(self.tasks_obj.keys())  # all task name
        self.task_deps = {t.name: list(t.deps) for t in self.tasks_obj.values()}  # task deps name
        self._task_flows = self.gen_flows(self.task_deps)

        self.get_source_deps()
        self._source_flows = self.gen_flows(self.source_deps)

        self.map_task_names = self._map_task_name()

    def gen_flows(self, deps):
        tasks = self.reverse_tasks(deps)
        task_flows = self._gen_flows(tasks, tasks)
        return task_flows

    def links(self, key, flows, linked_tasks):
        children = flows[key]
        res = []
        self.to_link(children, res)
        res.insert(0, key)
        linked_tasks[key] = res

    def _get_task(self, key, flows_, flows, linked_tasks, task_deps):
        if key not in flows_:
            raise KeyError('{} not in data'.format(key))

        if key not in linked_tasks:
            self.links(key, flows_, linked_tasks)

        tasks_link = linked_tasks[key]
        deps = dict()
        tasks = list()

        for task in tasks_link:
            deps_ = task_deps.get(task, [])
            tasks.append(task)
            deps[task] = [d for d in deps_ if d in tasks_link]
        flows[key] = (list(tasks), deps)

    def get_task(self, name):
        key = self._concat_names(name)
        if key not in self.task_flows:
            if isinstance(name, (str, int)):
                self._get_task(name, self._task_flows, self.task_flows, self.linked_tasks, self.task_deps)
            else:
                self._get_tasks(name, self._task_flows, self.task_flows, self.linked_tasks, self.task_deps)
        return self.task_flows[key]

    def get_source_task(self, name):
        key = self._concat_names(name)
        if key not in self.source_flows:
            if isinstance(name, (str, int)):
                self._get_task(name, self._source_flows, self.source_flows,
                               self.linked_source, self.source_deps)
            else:
                self._get_tasks(name, self._source_flows, self.source_flows,
                                self.linked_source, self.source_deps)
        return self.source_flows[key]

    def _get_tasks(self, names, flows_, flows, linked_tasks, task_deps):
        key_ = self._concat_names(names)
        tasks_all = set()
        deps_all = {}
        for name in names:
            if name not in flows:
                self._get_task(name, flows_, flows, linked_tasks, task_deps)
            tasks, deps = flows[name]
            tasks_all.update(tasks)
            for key, val in deps.items():
                if key not in deps_all:
                    deps_all[key] = val
                else:
                    val = set(deps_all[key] + val)
                    deps_all[key] = list(val)
        flows[key_] = (list(tasks_all), deps_all)

    def get_task_flow(self, names=None, flow_name=None, suffix=FLOW_PREFIX):
        key = self._concat_names(names)
        if flow_name is None:
            flow_name = suffix + self._concat_names(names)
        tasks, deps = self.get_task(names)
        flow = Flow(flow_name, tasks, deps)

        self._flow = flow
        return flow

    def get_source_flow(self, names, flow_name=None, suffix=SOURCE_FLOW_PREFIX):
        key = self._concat_names(names)
        if flow_name is None:
            flow_name = suffix + self._concat_names(names)
        tasks, deps_ = self.get_source_task(names)

        tasks = [t for t in tasks if t in self.tasks]
        deps = {}
        for k, v in deps_.items():
            if k in tasks:
                v = [t for t in v if t in self.tasks]
                deps[k] = v
        # deps = {k: v for k, v in deps.items() if k in tasks}
        flow = Flow(flow_name, tasks, deps)

        self._source_flow = flow
        return flow

    def dump_task_flow(self, names=None, flow_name=None):
        flow = self.get_task_flow(names, flow_name)
        flow = flow.uniform()
        flow.dump()

    def dump_source_flow(self, names=None, flow_name=None):
        flow = self.get_source_flow(names, flow_name)
        flow = flow.uniform()
        flow.dump()

    def _run_flow(self, names, suffix, **kwargs):
        engine = Engine(self.context)

        flow_name = kwargs.get('flow_name')
        if not flow_name:
            flow_name = suffix + self._concat_names(names)
        force = kwargs.get('force')
        return engine.execute_async(flow_name=flow_name, force=force)

    def run_task_flow(self, names, **kwargs):
        return self._run_flow(names, suffix=FLOW_PREFIX, **kwargs)

    def run_source_flow(self, names, **kwargs):
        return self._run_flow(names, suffix=SOURCE_FLOW_PREFIX, **kwargs)

    def _rm_flow(self, names=None, flow_name=None, suffix=None):
        if flow_name is None:
            key = self._concat_names(names)
            flow_name = suffix + key
        flowstore = self.context.get_flowstore()
        flowstore.delete(flow_name)

    def rm_task_flow(self, names=None, flow_name=None):
        return self._rm_flow(names, flow_name, suffix=FLOW_PREFIX)

    def rm_source_flow(self, names=None, flow_name=None):
        return self._rm_flow(names, flow_name, suffix=SOURCE_FLOW_PREFIX)

    def _store_task_flow(self, names, flow_name=None):
        key = self._concat_names(names)
        if flow_name is None:
            flow_name = FLOW_PREFIX + key

        tasks, deps = self.get_task(names)

        # drop
        tasks = [t for t in tasks if t != flow_name]
        deps = {k: v for k, v in deps.items() if v}
        return flow_name, tasks, deps

    def store_task_flow(self, names, flow_name=None):
        flow_name, tasks, deps = self._store_task_flow(names, flow_name=flow_name)
        self._show_flow(flow_name, tasks, deps)

    def store_source_flow(self, names, flow_name=None):
        flow_name, tasks, deps = self._store_source_flow(names, flow_name=flow_name)
        self._show_flow(flow_name, tasks, deps)

    def _store_source_flow(self, names, flow_name=None):
        key = self._concat_names(names)
        if flow_name is None:
            flow_name = SOURCE_FLOW_PREFIX + key

        tasks, deps_ = self.get_source_task(names)

        #  drop
        tasks = [t for t in tasks if t in self.tasks]
        deps = {}
        for k, v in deps_.items():
            if k in tasks:
                v = [t for t in v if t in self.tasks]
                if v:
                    deps[k] = v

        return flow_name, tasks, deps

    def _show_flow(self, flow_name, tasks, deps):
        flowstore = self.context.get_flowstore()
        flowstore.create(flow_name, *tasks, deps=deps)

        print('Flow {} created, details:'.format(flow_name))
        flow = flowstore.load(flow_name).uniform()
        print('tasks [{}]: {}'.format(len(flow.tasks), flow.tasks))
        print('dependencies:')
        print('------------------------------------------')
        flow.dump()
        print('------------------------------------------')

    @property
    def source_pattern(self):
        pattern = self._source_pattern
        if pattern is None:
            pattern = "{}\(\s*[\'\"](.*?)[\'\"]\s*,"
        return pattern

    @source_pattern.setter
    def source_pattern(self, value):
        self._source_pattern = value

    def gen_pattern(self, pattern_key):
        """
        :pattern_key: str or list or tuple
        """
        pattern = self.source_pattern
        if pattern_key:
            pattern = [pattern.format(arg) for arg in pattern_key]
        pattern_c = re.compile('|'.join(pattern))
        self.pattern = pattern_c
        return pattern_c

    def get_source(self, name, pattern_key=('get_stat', 'context.load')):
        if not isinstance(name, str):
            raise TypeError('name except str, {} got'.format(type(name).__name__))
        if not isinstance(pattern_key, (tuple, list)):
            raise TypeError('pattern_key except tuple or list, {} got'.format(
                            type(name).__name__))

        task = self.context.get_task(name)
        lines = inspect.getsourcelines(task.__class__)
        source_code = self.drop_comments(lines)

        if self.pattern is None:
            pattern = self.gen_pattern(pattern_key)
        else:
            pattern = self.pattern

        items = pattern.findall(source_code)
        source = set(flatten(items))

        return list(source)

    def get_source_deps(self):
        '''
        genarete tables/tasks and deps
        '''
        for task in self.tasks:
            if task not in self.source_deps:
                self.source_deps[task] = self.get_source(task)

    def bfs(self, tasks, targets):
        q = Queue()
        s = set()

        targets = set(targets)
        [q.put(task) or s.add(task) for task in targets]

        while not q.empty():
            task = q.get()
            yield task
            for t in tasks.get(task):
                if t not in s:
                    q.put(t)
                    s.add(t)

    def _map_task_name(self):
        tasks = {}
        for key, val in self.tasks_obj.items():
            tasks[key] = val.__module__.split('.')[-1]
        return tasks

    def map_source_task_name(self, name):
        key = self._concat_names(name)
        tasks, _ = self.get_source_task(key)
        return {task: self.map_task_names.get(task, '') for task in tasks}

    def store_source_task_name(self, name):
        '''
            store task file name and task's runtime name
        '''
        dirname = 'task_map_name'
        key = self._concat_names(name)
        tasks = self.map_source_task_name(name)
        if not os.path.exists(dirname):
            os.mkdir(dirname)
        filename = os.path.join(dirname, key)
        with open(filename, 'w+') as f:
            for key, val in tasks.items():
                line = '{:<35s}{}\n'.format(key, val)
                f.write(line)

    def rm_source_task_name(self, name, force=False):
        key = self._concat_names(name)
        dirname = 'task_map_name'
        filename = os.path.join(dirname, key)
        os.remove(filename)
        if not os.listdir(dirname) or force:
            os.rmdir(dirname)

    @classmethod
    def to_link(cls, task_flow, res):
        '''BFS'''
        for key in task_flow.keys():
            if key and key not in res:
                res.append(key)
        t = dict([(k, v) for val in task_flow.values() for k, v in val.items()])
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

    @staticmethod
    def reverse_tasks(deps):
        '''exchange deps and task'''
        res = {}
        for key, vals in deps.items():
            for val in vals:
                if val not in res:
                    res[val] = {}
                res[val][key] = {}
            if key not in res:
                res[key] = {}
        return res

    @classmethod
    def _gen_flows(cls, tasks, all_tasks):
        res = {}
        for key, vals in tasks.items():
            res[key] = {}
            if key in all_tasks:
                res[key] = cls._gen_flows(all_tasks[key], all_tasks)
        return res

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
