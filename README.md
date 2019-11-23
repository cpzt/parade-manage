# Parade-manage

`Parade-manage` is moudle for parade.

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
	
manage = ParadeManage() # or manage = ParadeManage(path='/your/parade/project/path')
```
If task a is failed, you can get task a's subtask and deps, like 

```bash
> manage.get_task('a') # get task a
([a, d, e, g, h], {'a': [], 'd': [a], 'e': ['a'], 'g': ['d', 'e'], 'h': [e]})
```
The first result is tasks, other is deps. And also, you can get mutil failed tasks.

```bash
> manage.get_task(['d', 'e']) # get task d and e
(['d', 'e', 'g', 'h'], {'d':[], 'e': [], 'g': ['d', 'e'], 'h': ['e']})
```
Then, create flow and store flow, generate a yaml file
```bash
> manage.store_task_flow('a') # args: 'a' or ['d', 'e']  
```
Or, remove flow by task name(s)
```bash
> manage.rm_task_flow('a')
```
Run the flow, execute failed tasks
```
manage.run_taskflow('a')
```
Sometimes, A task does not depend on other tasks, but uses some tables.
Now you can get the tables and tasks.
```bash
> manage.get_source('d')
['t1', 'a']  # return table 't1' and task name 'a'
```
And if table `t1` is failed, you can use `store_source_flow` to store flow
```bash
> manage.store_source_flow('t1')
# return a flow related to etl task only
```
Store DAG to file
```bash
>>> manage.store_to_file('analysis.task')
# return a dag
```
