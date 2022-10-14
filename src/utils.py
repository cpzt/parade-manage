
try:
    # Python <= 3.9
    from collections import Iterable
except ImportError:
    # Python > 3.9
    from collections.abc import Iterable

import inspect
from importlib import import_module

from pkgutil import iter_modules
from typing import List, Type, Callable, Dict


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


def tree(item_map: Dict[str, List], name: str, prefix: str = "", is_root: bool = True, is_tail: bool = True):
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
