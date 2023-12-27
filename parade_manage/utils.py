from __future__ import annotations

try:
    # Python <= 3.9
    from collections import Iterable
except ImportError:
    # Python > 3.9
    from collections.abc import Iterable

from collections import defaultdict

import inspect
from importlib import import_module

from pkgutil import iter_modules
from typing import List, Type, Callable, Dict, Set, Tuple


def walk_modules(path: str) -> List:
    """
    :param path: e.g.  your.project
    :return : e.g. ["your.project.some"]
    """
    mods = []

    mod = import_module(path)

    if not hasattr(mod, '__path__'):
        return []

    for finder, sub_path, is_pkg in iter_modules(mod.__path__):
        full_path = path + "." + sub_path
        if is_pkg:
            mods.extend(walk_modules(full_path))
        else:
            mod = import_module(full_path)
            mods.append(mod)

    return mods


def iter_classes(base_class: Type, *modules, class_filter: Callable = None):
    for root_module in modules:
        try:
            mods = walk_modules(root_module)
        except Exception as e:
            raise e

        for mod in mods:
            for class_obj in vars(mod).values():
                if inspect.isclass(class_obj) and issubclass(class_obj, base_class) \
                        and class_obj.__module__ == mod.__name__:
                    if not class_filter or class_filter(class_obj):
                        yield class_obj


def flatten(items, ignore_types=(bytes, str), ignore_flags=('', None)):
    for item in items:
        if item in ignore_flags:
            continue
        if isinstance(item, Iterable) and not isinstance(item, ignore_types):
            yield from flatten(item)
        else:
            yield item


def tree(item_map: Dict[str, List[str]], name: str, prefix: str = "", is_root: bool = True, is_tail: bool = True):
    if is_root:
        print(prefix + " " + name)
    else:
        print(prefix + ("└── " if is_tail else "├── ") + name)

    children = item_map.get(name, [])

    if len(children) > 0:
        last_child = children[-1]
        rest_child = children[0:-1]
        for child in rest_child:
            tree(item_map, child, prefix + ("    " if is_tail else "│   "), False, False)
        tree(item_map, last_child, prefix + ("    " if is_tail else "│   "), False, True)


def find_cycles(graph: Dict[str, List[str] | Set[str]]) -> List[List[str]]:
    visited = set()
    recursion_stack = set()
    cycles = []

    def dfs(node, path):
        if node in recursion_stack:
            cycles.append(path + [node])
            return

        if node in visited:
            return

        visited.add(node)
        recursion_stack.add(node)

        if node in graph:
            for neighbor in graph[node]:
                dfs(neighbor, path + [node])

        recursion_stack.remove(node)

    for node in graph:
        dfs(node, [])

    return cycles


def check(tasks: Dict[str, List[str] | Set[str]]) -> Tuple[Dict, Dict, Dict, List]:

    non_deps_tasks: Dict[str, Set[str]] = defaultdict(set)
    duplicate_tasks: Dict[str, Set[Tuple[str, int]]] = defaultdict(set)
    circular_tasks: List[List[str]] = find_cycles(tasks)

    for task, deps in tasks.items():
        for dp in deps:
            # check for invalid dependencies
            if dp not in tasks:
                non_deps_tasks[task].add(dp)

            # check for duplicate dependencies
            if deps.count(dp) > 1:
                duplicate_tasks[task].add((dp, deps.count(dp)))

    non_deps_tasks = {k: set(v) for k, v in non_deps_tasks.items()}
    duplicate_tasks = {k: set(v) for k, v in duplicate_tasks.items()}

    return tasks, non_deps_tasks, duplicate_tasks, circular_tasks


def show_check_info(tasks: Dict[str, List[str] | Set[str]]):
    deps, non_deps, duplicate, circular = check(tasks)

    if len(non_deps) == 0 and len(duplicate) == 0 and len(circular) == 0:
        print(f"Total: {len(deps)}, PASS")
        return
    else:
        print(f"Total: {len(deps)}")

    dividing_line = "---*---" * 8
    print(dividing_line)

    if len(non_deps) > 0:
        print("[Invalid Dependencies]")
        for k, v in non_deps.items():
            print(k, ' ==>  ', v)
        print("\n" + dividing_line)

    if len(duplicate) > 0:
        print('[Duplicate Dependencies]')
        for k, v in duplicate.items():
            print(k, ' ==>  ', v)
        print("\n" + dividing_line)

    if len(circular) > 0:
        print('[Circular Dependencies]')
        for cycle in circular:
            print(" -> ".join(cycle))
        print("\n" + dividing_line)
