import inspect
from typing import Set, List, Type, Callable


class Task:

    @property
    def name(self):
        return self.__class__.name

    @property
    def deps(self):
        return set()

    def run(self):
        raise NotImplementedError


def add_task(name: str = None, deps: str = None, exec_fun: Callable = None, super_class: Type = Task) -> Type:
    cls = type(name, (super_class,), {"deps": deps, "name": name, "run": exec_fun})
    return cls


def task(name: str, deps: List | Set | None = None, super_class: Type = Task) -> Callable[..., Callable[[Type | Callable], Type]]:
    """
       Decorator for creating Task

       Parameters:
       - name (str): The name of the task.
       - deps (List | Set | None, optional): Set of dependencies for the task. If not provided, it defaults to an empty set.
       - super_class (Type): The super class

       Returns:
       - Callable[..., Callable[[Type | Callable], Type]]: A decorator that takes either a class or a callable function and returns a new class or adds the function as a task.

       Usage:
       - To decorate a class as a task:
         ```python
         @task(name="my_task", deps=["dependency1", "dependency2"], super_class=Task)
         class MyTask(Task):
             # Class body
         ```

       - To decorate a function as a task:
         ```python
         @task(name="my_function_task", deps={"dependency1", "dependency2"}, super_class=Task)
         def my_function():
             # Function body
         ```
       """
    def wrap(f: Type | Callable) -> Type:
        if inspect.isclass(f):
            if issubclass(f, Task):
                return f
            else:
                base_classes = [Task] + list(cls for cls in f.__bases__)
                attr = f.__dict__
                attr.deps = deps or attr.get('deps', set())
                return type(name or f.__name__, tuple(base_classes), dict(attr))
        elif inspect.isfunction(f):
            return add_task(name=f.__name__, deps=deps, exec_fun=f, super_class=super_class)
        else:
            raise RuntimeError("only support function or class")
    return wrap
