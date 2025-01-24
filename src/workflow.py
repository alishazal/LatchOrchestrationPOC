import networkx as nx
import graphviz
from typing import List, Optional
import os

from src.constraints import TaskConstraints

###############################################################################
# Task + Workflow (Developer Side)
###############################################################################

class Task:
    def __init__(self, task_id: str, func, constraints: TaskConstraints=None, metadata: dict=None):
        self.task_id = task_id
        self.func = func  # signature: func(wf_id, task_id, inputs, orchestrator)
        self.constraints = constraints or TaskConstraints()
        self.metadata = metadata or {}

    def __repr__(self):
        return f"Task({self.task_id})"

def check_node_against_policy(taskA: Task, taskB: Task, direction='next'):
    """
    If direction='next', check taskA's valid_next_nodes. 
    If direction='previous', check taskB's valid_previous_nodes.
    """
    if direction == 'next':
        policy = taskA.constraints.valid_next_nodes_policy
        if policy == 'allow_all':
            return
        if policy == 'allow_none':
            raise RuntimeError(f"Task {taskA.task_id} cannot spawn any child nodes.")
        if policy == 'custom':
            # we must find taskB in taskA.constraints.valid_next_nodes
            if taskB.task_id not in taskA.constraints.valid_next_nodes:
                raise RuntimeError(f"Task {taskA.task_id} not allowed to spawn {taskB.task_id}.")
    else:
        # direction='previous'
        policy = taskB.constraints.valid_previous_nodes_policy
        if policy == 'allow_all':
            return
        if policy == 'allow_none':
            raise RuntimeError(f"Task {taskB.task_id} cannot have any parents.")
        if policy == 'custom':
            if taskA.task_id not in taskB.constraints.valid_previous_nodes:
                raise RuntimeError(f"Task {taskB.task_id} not allowed to have {taskA.task_id} as a creator.")

def check_edge_against_policy(taskA: Task, taskB: Task, direction='outgoing'):
    """
    If direction='outgoing', check that taskA's valid_outgoing_edges policy. Allows edge (taskA->taskB).
    If direction='incoming', check that taskA's valid_incoming_edges policy. Allows edge (taskB->taskA).
    """
    if direction == 'outgoing':
        policy = taskA.constraints.valid_outgoing_edges_policy
        valid_edges = taskA.constraints.valid_outgoing_edges
    else:  # incoming
        policy = taskA.constraints.valid_incoming_edges_policy
        valid_edges = taskA.constraints.valid_incoming_edges

    if policy == 'allow_all':
        return
    if policy == 'allow_none':
        raise RuntimeError(f"Task {taskA.task_id} cannot create {direction} edges.")
    if policy == 'custom':
        # must appear in policy.custom_edges
        if policy == 'outgoing' and (taskA.task_id, taskB.task_id) not in valid_edges:
            raise RuntimeError(f"Edge {taskA.task_id}->{taskB.task_id} not in valid_outgoing_edges of {taskA.task_id}.")
        if policy == 'incoming' and (taskB.task_id, taskA.task_id) not in valid_edges:
            raise RuntimeError(f"Edge {taskA.task_id}->{taskB.task_id} not in valid_incoming_edges of {taskA.task_id}.")

class Workflow:
    """
    Add tasks, edges, constraints.
    """
    def __init__(self, workflow_id: str):
        self.workflow_id = workflow_id
        self.tasks = {}   # task_id -> Task
        self.edges = []   # (parent, child)
        self.visual_edges = [] # (parent, child)
        self.visualization_counter = 1

    def add_task(self, task: Task, dependencies: Optional[List[str]]=None):
        """
        Register a new task, check no cycles, partial constraints.
        """
        if task.task_id in self.tasks:
            raise ValueError(f"Task {task.task_id} already in {self.workflow_id}")

        self.tasks[task.task_id] = task

        # add edges
        if dependencies:
            for dep in dependencies:
                if dep not in self.tasks:
                    raise ValueError(f"Dependency {dep} not in {self.workflow_id}")
                check_node_against_policy(self.tasks[dep], task, direction='next')
                check_node_against_policy(self.tasks[dep], task, direction='previous')
                check_edge_against_policy(task, self.tasks[dep], direction='incoming')
                check_edge_against_policy(self.tasks[dep], task, direction='outgoing')
                self.edges.append((dep, task.task_id))
                self.visual_edges.append((dep, task.task_id))

        # Not Implemented: there should be a check on the max_nodes registered against the node

        self._assert_no_cycle()
        self.visualize()
        self.visualization_counter += 1

    def validate_constraints(self):
        pass

    def _assert_no_cycle(self):
        g = nx.DiGraph()
        for t_id in self.tasks:
            g.add_node(t_id)
        for (p,c) in self.edges:
            g.add_edge(p,c)
        if not nx.is_directed_acyclic_graph(g):
            raise ValueError("DAG cycle detected in workflow registration.")

    def visualize(self):
        """
        Called by developer to see the DAG as they build it
        """
        dot = graphviz.Digraph(name=f'{self.workflow_id}_{self.visualization_counter}', format='png')
        dot.attr(rankdir='LR')
        
        # for known tasks and edges
        for t_id, t_obj in self.tasks.items():
            label = f"{t_id}"
            if t_obj.metadata:
                for key, val in t_obj.metadata.items():
                    label += f"\\n{key} = {val}"
            dot.node(t_id, label=label)

        for (p,c) in self.visual_edges:
            dot.edge(p,c)
        
        # for TBD tasks and edges
        shown_map_nodes = set()  # Track which map patterns we've shown
        for t_id, t_obj in self.tasks.items():
            if t_obj.constraints.valid_next_nodes_policy == 'custom':
                for cnode in t_obj.constraints.valid_next_nodes:
                    if not self.tasks.get(cnode):
                        # Check if this is a map-reduce pattern
                        if t_obj.metadata.get('type', '') == 'map_reduce_starter':
                            base_name = cnode.rsplit('_', 1)[0]  # Get everything before the last underscore
                            if base_name not in shown_map_nodes:
                                shown_map_nodes.add(base_name)
                                example_node = f"{base_name}_N"
                                dot.node(example_node, label=f"{example_node}\\n(TBD)", style="dashed", color="gray")
                                if (t_obj.constraints.valid_outgoing_edges_policy == 'allow_all' or \
                                    (t_obj.constraints.valid_outgoing_edges_policy == 'custom' and (t_id, cnode) in t_obj.constraints.valid_outgoing_edges)):
                                    dot.edge(t_id, example_node, style="dashed", color="gray")
                        else:
                            # For non-map nodes (like branches), show them all
                            dot.node(cnode, label=f"{cnode}\\n(TBD)", style="dashed", color="gray")
                            if (t_obj.constraints.valid_outgoing_edges_policy == 'allow_all' or \
                                (t_obj.constraints.valid_outgoing_edges_policy == 'custom' and (t_id, cnode) in t_obj.constraints.valid_outgoing_edges)):
                                dot.edge(t_id, cnode, style="dashed", color="gray")

        filename = f'./demo_pngs/{self.workflow_id}/{self.workflow_id}_Register_{self.visualization_counter}'
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        dot.render(filename, cleanup=True)
        print(f"[Workflow] rendered {filename}.png")