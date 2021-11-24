# -*- coding: utf-8 -*-

"""
parade manager for managing `parade`
"""
import os
import re
import inspect

from collections import Iterable
from queue import Queue

from parade.core.task import Flow, Task
from parade.core.context import Context
from parade.core.engine import Engine
from parade.utils.workspace import load_bootstrap
from parade.utils.modutils import iter_classes
from parade.error.task_errors import DuplicatedTaskExistError


FLOW_PREFIX = 'flow_'
SOURCE_FLOW_PREFIX = 'source_flow_'
SOURCE_PREFIX = 'source_'


class ParadeManage:

    def __init__(self, path=None, target_path=None):
        self.linked_tasks = {}
        self.linked_source = {}
        self.task_flows = {}
        self.source_flows = {}
        self.source_deps = {}
        self._source_pattern = None
        self.pattern = None
        self.target_path = target_path

        self.path = path
        if path:
            os.chdir(path)

        boot = load_bootstrap()
        self.context = Context(boot)
        self.tasks_obj = self.context.load_tasks(self.target_path)  # all task object
        self.tasks = list(self.tasks_obj.keys())  # all task name
        self.task_deps = {t.name: list(t.deps) for t in self.tasks_obj.values()}  # task deps name
        self._task_flows = self.gen_flows(self.task_deps)

        self.get_source_deps()
        self.map_task_names = self._map_task_name()

    def map_filename(self):
        """
        :return: `key` is task name , `value` is file name
        """
        return {k: str(type(v)).split('.')[-2] for k, v in self.tasks_obj.items()}

    def gen_flows(self, deps):
        tasks = self.reverse_tasks(deps)
        
        task_flows = self._gen_flows(tasks, tasks)
        return task_flows

    def links(self, key, flows, linked_tasks):
        children = flows[key]
        res = []
        self.to_link(children, res)
        list.insert(res, 0, key)
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
        if flow_name is None:
            flow_name = suffix + self._concat_names(names)
        tasks, deps = self.get_task(names)
        flow = Flow(flow_name, tasks, deps)

        return flow

    def get_source_flow(self, names, flow_name=None, suffix=SOURCE_FLOW_PREFIX):
        if flow_name is None:
            flow_name = suffix + self._concat_names(names)
        tasks, deps_ = self.get_source_task(names)

        tasks = [t for t in tasks if t in self.tasks]
        deps = {}
        for k, v in deps_.items():
            if k in tasks:
                v = [t for t in v if t in self.tasks]
                deps[k] = v
        flow = Flow(flow_name, tasks, deps)

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
        flow_store = self.context.get_flowstore()
        flow_store.delete(flow_name)

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
        flow_store = self.context.get_flowstore()
        flow_store.create(flow_name, *tasks, deps=deps)

        print('Flow {} created, details:'.format(flow_name))
        flow = flow_store.load(flow_name).uniform()
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

        source = set(list(flatten(items)) + self.task_deps[task.name])
        
        return list(source)

    def get_source_deps(self):
        """
        generate tables/tasks and deps
        """
        for task in self.tasks:
            if task not in self.source_deps:
                self.source_deps[task] = self.get_source(task)

    @staticmethod
    def bfs(tasks, targets):
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
        """
            store task file name and task's runtime name
        """
        dir_name = 'task_map_name'
        key = self._concat_names(name)
        tasks = self.map_source_task_name(name)
        if not os.path.exists(dir_name):
            os.mkdir(dir_name)
        filename = os.path.join(dir_name, key)
        with open(filename, 'w+') as f:
            for key, val in tasks.items():
                line = '{:<35s}{}\n'.format(key, val)
                f.write(line)

    def rm_source_task_name(self, name, force=False):
        key = self._concat_names(name)
        dir_name = 'task_map_name'
        filename = os.path.join(dir_name, key)
        os.remove(filename)
        if not os.listdir(dir_name) or force:
            os.rmdir(dir_name)

    @classmethod
    def to_link(cls, task_flow, res):
        """BFS"""
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
        """exchange deps and task"""
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
        for key in tasks.keys():
            res[key] = {}
            if key in all_tasks:
                res[key] = cls._gen_flows(all_tasks[key], all_tasks)
        return res

    def __enter__(self):
        return self

    def __exit__(self, exc_ty, exc_val, exc_tb):
        pass

    def __repr__(self):
        return '<ParadeManager(path={}, target_path={})>'.format(self.path, self.target_path)

    def store_to_file(self, path=None, task_class=Task):
        """
        :param path: dir path
        :param task_class: parade `Task` class

        :Example:

        >>>  m = ParadeManage()
        >>>  m.store_to_file('analysis.task')

        """
        if path is None:
            path = self.context.name + '.task'

        d = {}
        for task_class in iter_classes(task_class, path):
            task = task_class()
            task_name = task.name
            deps = task.deps

            if task_name in d:
                print([task_name], [task])
                raise DuplicatedTaskExistError(task=task_name)
            d[task_name] = deps

        tasks = list(d.keys())
        deps = {}
        for key, vals in d.items():
            tmp = []
            for val in vals:
                if val in tasks:
                    tmp.append(val)
            if len(tmp) > 0:
                deps[key] = tmp

        self._show_flow(path, tasks, deps)

    def gen_deps(self, task):
        if task not in self.task_deps:
            raise KeyError(f"no such task {task}")

        stack = [task]
        res = {}
        while stack:
            name = stack.pop(0)
            deps = list(self.task_deps.get(name, []))

            if name not in res:
                res[name] = deps
            stack.extend(deps)

        return res

    def gen_all_deps(self, tasks):
        res = {}
        for task in tasks:
            deps = self.gen_deps(task)
            res.update(deps)
        return res

    def store_all_deps(self, name, tasks):
        deps = self.gen_all_deps(tasks)
        self._show_flow('flow_' + name, list(deps.keys()), {k: v for k, v in deps.items() if len(v) > 0})


def flatten(items, ignore_types=(bytes, str), ignore_flags=('', None)):
    for item in items:
        if item in ignore_flags:
            continue
        if isinstance(item, Iterable) and not isinstance(item, ignore_types):
            yield from flatten(item)
        else:
            yield item
