import inspect


class Task:

    @property
    def name(self):
        return self.__class__.name

    @property
    def deps(self):
        return set()

    def run(self):
        raise NotImplementedError


def add_task(name=None, deps=None, exec_fun=None):
    cls = type(name, (Task,), {"deps": deps, "name": name, "run": exec_fun})
    return cls


def task(name=None, deps=None):
    def wrap(f):
        if inspect.isclass(f):
            if issubclass(f, Task):
                return f
            else:
                base_classes = [Task] + [cls for cls in f.__base_classes__]
                attr = f.__dict__
                attr['deps'] = deps or attr.get('deps', set())
                return type(name or f.__name__, tuple(base_classes), attr)
        elif inspect.isfunction(f):
            return add_task(name=f.__name__, deps=deps, exec_fun=f)
        else:
            raise RuntimeError("only support function or class")
    return wrap
