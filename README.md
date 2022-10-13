# Parade-manage

`Parade-manage` is module for parade.

**Note**: You should install [parade](https://github.com/bailaohe/parade) first.

## Install
Install is simple:
```bash
> pip install parade-manage
```
## Usage
Tasks dag:  
```
 t1  a   b   c
  \ / \ / \ / 
   d   e   f
    \ / \
     g   h
```
**Note**: ***t1*** is table name, other are task name
Enter your project first
```bash
> cd your_project
```
Initialize the class
```python
from parade_manage import ParadeManage
	
m = ParadeManage() # or m = ParadeManage(project_path='/your/parade/project/path')
```

dump and generate yaml file
```bash
> m.dump(target_tasks=["e", "f"], flow_name="flow")
```
flow.yml
```yaml 
deps:
  - e->a,b
  - f->b,c
tasks:
  - a
  - b
  - c
  - e
  - f
```

show tasks
```bash
> m.tree(name="flow", task_names=['d', 'e'])
```
