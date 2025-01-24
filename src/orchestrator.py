import networkx as nx
import graphviz
from typing import List, Tuple, Optional, Any
import time
from collections import deque
import os

from src.workflow import check_node_against_policy, check_edge_against_policy, Task, Workflow

###############################################################################
# The Orchestrator (To Run Workflows Sent by Users)
###############################################################################

def task_runner(wf_id, task_id, inputs, orchestrator):
    """
    Separate process function to run the actual task code
    """
    try:
        wf = orchestrator.workflows[wf_id]
        task_obj = wf.tasks[task_id]
        res = task_obj.func(wf_id, task_id, inputs, orchestrator)
        orchestrator.complete_task(wf_id, task_id, res)
    except Exception as e:
        print(f"[Runner] exception for {task_id}: {e}")
        orchestrator.task_status[(wf_id, task_id)] = 'failed'

class Orchestrator:
    """
    Store any given workflow, run any workflow, do runtime scheduling, check constraints at runtime for all spawns.
    """
    def __init__(self):
        self.workflows = {}  # wf_id -> Workflow
        self.ready_queue = {} # a queue for tasks that become unblocked

        self.task_status = {}  # (wf_id, task_id) -> "pending"/"running"/"done"/"failed"
        self.task_inputs = {}  # (wf_id, task_id) -> inputs
        self.task_outputs = {} # (wf_id, task_id) -> result
        self.spawn_counts = {} # (wf_id, task_id) -> int
        self.processes = {}   # (wf_id, task_id) -> Process
        self.running_loops = {} # wf_id -> bool; used to tore the status of a workflow; will be used to check if a workflow is already running
        self.run_counter = {} # wf_id -> int; to count how many nodes have been run; used in visualization for labelling
        self.execution_history = {} # (wf_id, task_id) -> int
        self.visualization_counter = {} # wf_id -> int; to count how many times the visualization has been run

    def register_workflow(self, wf: Workflow):
        """
        The user provides a workflow, we store it and init statuses
        """
        if wf.workflow_id in self.workflows:
            raise ValueError(f"Workflow {wf.workflow_id} already registered.")
        self.workflows[wf.workflow_id] = wf
        for t_id in wf.tasks:
            self.task_status[(wf.workflow_id, t_id)] = 'pending'
            self.spawn_counts[(wf.workflow_id, t_id)] = 0
        self.ready_queue[wf.workflow_id] = deque()
        self.running_loops[wf.workflow_id] = False
        self.run_counter[wf.workflow_id] = 0
        self.visualization_counter[wf.workflow_id] = 1
        print(f"[Orchestrator] Received workflow {wf.workflow_id} with {len(wf.tasks)} tasks, {len(wf.edges)} edges.")

    def run_workflow(self, wf_id: str, input_data: dict):
        """
        Single call that runs a continuous while loop until the workflow is done.
        We'll spawn tasks that have no deps, run them, detect new spawns, etc.
        """
        if wf_id not in self.workflows:
            raise ValueError(f"No workflow {wf_id}")

        if self.running_loops[wf_id]:
            raise RuntimeError(f"Workflow {wf_id} is already running a scheduling loop.")

        self.running_loops[wf_id] = True

        wf = self.workflows[wf_id]

        # find tasks with no incoming edges => put them in queue with input_data
        rev = {}
        for (p,c) in wf.edges:
            rev.setdefault(c,[]).append(p)
        for t_id in wf.tasks:
            if t_id not in rev:  # no dependencies
                self.ready_queue[wf_id].append((t_id, input_data.get(t_id, [])))

        # while loop
        while True:
            # schedule tasks that are unblocked
            self.run_helper(wf_id)

            # check if all tasks are done or failed
            statuses = [
                self.task_status[(wf_id, t)] for t in wf.tasks
            ]
            if all(s in ["done", "failed"] for s in statuses):
                print(f"[Orchestrator] All tasks are either done or failed for {wf_id}. Exiting loop.")
                break

            # small sleep
            time.sleep(1)

        self.running_loops[wf_id] = False

    def run_helper(self, wf_id: str):
        """
        If any tasks are in the queue => spawn them. Also discover newly unblocked tasks
        """
        wf = self.workflows[wf_id]
        rev = {}
        for (p,c) in wf.edges:
            rev.setdefault(c,[]).append(p)

        # discover newly unblocked tasks
        current_step = max(self.execution_history.get((wf_id, tid), 0) for tid in wf.tasks) + 1
        newly_unblocked = []
        
        for t_id in wf.tasks:
            key = (wf_id, t_id)
            if self.task_status[key] == 'pending':
                preds = rev.get(t_id, [])
                inputs = []
                all_done = True
                for p in preds:
                    if self.task_status[(wf_id, p)] in ['pending', 'running']:
                        all_done = False
                    
                    p_output = self.task_outputs.get((wf_id, p))
                    if p_output:
                        inputs.append(p_output)
                
                if all_done:
                    newly_unblocked.append((t_id, inputs))
        
        # Add all newly unblocked tasks at the same step
        for tid, inputs in newly_unblocked:
            self.ready_queue[wf_id].append((tid, inputs))
            self.execution_history[(wf_id, tid)] = current_step
        
        # spawn whatever is in queue
        while self.ready_queue[wf_id]:
            (tid, inputs) = self.ready_queue[wf_id].popleft()
            self.spawn_parallel(wf_id, tid, inputs)
            self.visualize(wf_id)

    def spawn_parallel(self, wf_id: str, t_id: str, inputs: Any):
        """
        Spawns a task in parallel. For simplicity of the POC we are running the task directly here, but in a real system we would launch a separate process.
        """
        key = (wf_id, t_id)
        if self.task_status[key] == 'pending':
            self.task_status[key] = 'running'
            self.task_inputs[key] = inputs  # Storing inputs for visualization
            task_runner(wf_id, t_id, inputs, self)

    def complete_task(self, wf_id: str, task_id: str, result: Any):
        """
        Called by the task process upon finishing
        """
        key = (wf_id, task_id)
        st = self.task_status[key]
        print(f"[Orchestrator] complete_task => {task_id} in {wf_id}, result={result}")
        if st not in ["running", "pending"]:
            return
        
        self.task_status[key] = 'done'
        self.task_outputs[key] = result

    def spawn_task(
        self,
        wf_id: str,
        creator_task_id: str,
        new_task: Task,
        new_edges: Optional[List[Tuple[str,str]]] = None,
        input_data: Any = None,
        skip_visual_edge: bool = False
    ):
        """
        Mid-run spawn. Check creator's constraints, new_task's constraints, cycle check, and schedule it in the queue.
        """
        def fail_workflow(error_msg: str):
            # Empty the ready queue
            self.ready_queue[wf_id].clear()
            
            # Mark all pending/running tasks as failed
            for task_id in self.workflows[wf_id].tasks:
                key = (wf_id, task_id)
                if self.task_status[key] in ['pending', 'running']:
                    self.task_status[key] = 'failed'
            
            # Stop the workflow loop
            self.running_loops[wf_id] = False
            
            # Raise the error
            raise RuntimeError(f"Workflow {wf_id} terminated due to constraint violation: {error_msg}")

        wf = self.workflows[wf_id]
        if creator_task_id not in wf.tasks:
            fail_workflow(f"Creator {creator_task_id} not in wf {wf_id}")

        creator_obj = wf.tasks[creator_task_id]

        # Check creator's max_spawn_count
        key = (wf_id, creator_task_id)
        if creator_obj.constraints.max_spawn_count is not None:
            if self.spawn_counts[key] >= creator_obj.constraints.max_spawn_count:
                fail_workflow(f"Task {creator_task_id} exceeded spawn count")

        # Check node constraints
        try:
            check_node_against_policy(creator_obj, new_task, direction='next')
            check_node_against_policy(creator_obj, new_task, direction='previous')
        except RuntimeError as e:
            fail_workflow(str(e))

        # Insert the new task
        if new_task.task_id in wf.tasks:
            fail_workflow(f"Task {new_task.task_id} already in wf {wf_id}")

        wf.tasks[new_task.task_id] = new_task
        self.task_status[(wf_id, new_task.task_id)] = 'pending'
        self.spawn_counts[(wf_id, new_task.task_id)] = 0
        self.spawn_counts[key] += 1

        parent_step = self.execution_history.get((wf_id, creator_task_id), 0)
        self.execution_history[(wf_id, new_task.task_id)] = parent_step + 1

        if not skip_visual_edge:
            wf.visual_edges.append((creator_task_id, new_task.task_id))

        # Add new edges
        if new_edges:
            for (src, dst) in new_edges:
                # Make sure the edge is related to atleast the parent_task or new_task
                if src not in [creator_task_id, new_task.task_id] and dst not in [creator_task_id, new_task.task_id]:
                    fail_workflow(f"The edge ({src}, {dst}) is not related to the creator or the new task.")

                src_object = wf.tasks[src]
                dst_object = wf.tasks[dst]

                try:
                    # Check edge constraints
                    check_edge_against_policy(dst_object, src_object, direction='incoming')
                    check_edge_against_policy(src_object, dst, direction='outgoing')
                except RuntimeError as e:
                    fail_workflow(str(e))
                    
                wf.edges.append((src,dst))
                wf.visual_edges.append((src,dst))

        # now do cycle check
        if not self.is_acyclic(wf):
            fail_workflow("spawn_task caused cycle")
        
        if input_data is not None:
            has_deps = any(dst == new_task.task_id for _, dst in wf.edges)
            if not has_deps:
                self.ready_queue[wf_id].append((new_task.task_id, input_data))

        print(f"[Orchestrator] spawn_task => added {new_task.task_id} from {creator_task_id}, new_edges={new_edges}")

    def check_task_status(self, wf_id: str, task_id: str):
        """
        Check status of a taks
        """
        s = self.task_status.get((wf_id, task_id), "unknown")
        r = None
        if s == 'done':
            r = self.task_outputs.get((wf_id, task_id))
        return {"status": s, "result": r}

    def visualize(self, wf_id: str):
        """
        Generate a visualization of the DAG
        """
        if wf_id not in self.workflows:
            return
        wf = self.workflows[wf_id]
        dot = graphviz.Digraph(name=wf_id, format='png')
        dot.attr(rankdir='LR')

        for t_id, t_obj in wf.tasks.items():
            s = self.task_status[(wf_id, t_id)]
            label = f"{t_id}\\n({s})"
            
            # Add execution step if task has started
            if s != "pending":
                execution_step = self.execution_history[(wf_id, t_id)]
                label += f"\\nStep {execution_step}"
            
            # Add inputs if task has started
            key = (wf_id, t_id)
            if key in self.task_inputs:
                label += f"\\nInputs: {str(self.task_inputs[key])}"
            
            # Add outputs if task is done
            if s == "done" and key in self.task_outputs:
                label += f"\\nReturn Value: {str(self.task_outputs[key])}"
                
            # Add metadata if present
            if t_obj.metadata:
                for key, val in t_obj.metadata.items():
                    label += f"\\n{key} = {val}"
                    
            dot.node(t_id, label=label)

        for (p,c) in wf.visual_edges:
            dot.edge(p,c)

        # for TBD tasks and edges
        for t_id, t_obj in wf.tasks.items():
            if t_obj.constraints.valid_next_nodes_policy == 'custom':
                shown_map_nodes = set()  # Track which map patterns we've shown
                for cnode in t_obj.constraints.valid_next_nodes:
                    if not wf.tasks.get(cnode):
                        # Check if this is a map-reduce
                        if t_obj.metadata.get('type', '') == 'map_reduce_starter':
                            base_name = cnode.rsplit('_', 1)[0]  # Get everything before the last underscore
                            if base_name not in shown_map_nodes:
                                shown_map_nodes.add(base_name)
                                example_node = f"{base_name}_N"
                                dot.node(example_node, label=f"{example_node}\\n(Not Run)", style="dashed", color="gray")
                                if (t_obj.constraints.valid_outgoing_edges_policy == 'allow_all' or \
                                    (t_obj.constraints.valid_outgoing_edges_policy == 'custom' and (t_id, cnode) in t_obj.constraints.valid_outgoing_edges)):
                                    dot.edge(t_id, example_node, style="dashed", color="gray")
                        else:
                            # For non-map nodes (like branches), show them all
                            dot.node(cnode, label=f"{cnode}\\n(Not Run)", style="dashed", color="gray")
                            if (t_obj.constraints.valid_outgoing_edges_policy == 'allow_all' or \
                                (t_obj.constraints.valid_outgoing_edges_policy == 'custom' and (t_id, cnode) in t_obj.constraints.valid_outgoing_edges)):
                                dot.edge(t_id, cnode, style="dashed", color="gray")

        filename = f'./demo_pngs/{wf_id}/{wf_id}_Run_{self.visualization_counter[wf_id]}'
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        dot.render(filename, cleanup=True)
        self.visualization_counter[wf_id] += 1
        print(f"[Orchestrator] rendered runtime to {filename}.png")

    def is_acyclic(self, wf: Workflow):
        """
        Check if the workflow is acyclic
        """
        g = nx.DiGraph()
        for t_id in wf.tasks:
            g.add_node(t_id)
        for (p,c) in wf.edges:
            g.add_edge(p,c)
        return nx.is_directed_acyclic_graph(g)
