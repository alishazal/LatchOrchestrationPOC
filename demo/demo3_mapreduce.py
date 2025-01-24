from src.orchestrator import Orchestrator
from src.constraints import TaskConstraints
from src.workflow import Task, Workflow

GLOBAL_TASKS = {}

###############################################################################
# 1. Developer: Create Task Funcs
###############################################################################

def map_reduce_starter_func(wf_id, task_id, inputs, orchestrator):
    all_map_tids = []
    for i, val in enumerate(inputs):
        map_tid = f"{task_id}_Map_{i+1}"
        all_map_tids.append(map_tid)
        map_constraints = TaskConstraints()
        map_task = Task(map_tid, map_func, constraints=map_constraints)
        orchestrator.spawn_task(wf_id=wf_id, creator_task_id=task_id, new_task=map_task, input_data=val)

    reduce_tid = f"{task_id}_Reduce"
    reduce_valid_previous_nodes = all_map_tids + [task_id]
    reduce_constraints = TaskConstraints(
        valid_previous_nodes_policy='custom',
        valid_previous_nodes=reduce_valid_previous_nodes, # the reduce task can have mapreduce and map nodes be able to interact with it
        valid_incoming_edges_policy='custom',
        valid_incoming_edges=[(m, reduce_tid) for m in all_map_tids]
    )
    reduce_task = Task(reduce_tid, reduce_func, constraints=reduce_constraints)
    orchestrator.spawn_task(wf_id=wf_id, creator_task_id=task_id, new_task=reduce_task, new_edges=[(m, reduce_tid) for m in all_map_tids])

def map_func(wf_id, task_id, inputs, orchestrator):
    result = sum(inputs) if isinstance(inputs, list) else inputs
    result *= 2
    return result

def reduce_func(wf_id, task_id, inputs, orchestrator):
    result = sum(inputs) if isinstance(inputs, list) else inputs
    return result


###############################################################################
# 2. Developer: Create Tasks + Workflow
###############################################################################

def build_demo_workflow():
    # 2.1 Creating the tasks
    global GLOBAL_TASKS

    # Map Reduce Starter Task
    map_reduce_start_spawn_count = 8
    map_reduce_starter_valid_next_nodes = [f'MapReduceStarter_Map_{i}' for i in range(1, map_reduce_start_spawn_count + 1)] + [f'MapReduceStarter_Reduce']
    map_reduce_starter_constraints1 = TaskConstraints(
        max_spawn_count=map_reduce_start_spawn_count,
        valid_next_nodes_policy='custom',
        valid_next_nodes=map_reduce_starter_valid_next_nodes
    )
    map_reduce_starter_task = Task("MapReduceStarter", func=map_reduce_starter_func, constraints=map_reduce_starter_constraints1, metadata={'type': 'map_reduce_starter'})
    GLOBAL_TASKS['MapReduceStarter'] = map_reduce_starter_task

    # 2.2 Build the workflow
    wf = Workflow("Demo3_MapReduce")
    wf.add_task(map_reduce_starter_task)

    return wf

###############################################################################
# 3. User: Run the Workflow with Input Data
###############################################################################
if __name__=="__main__":
    wf = build_demo_workflow()

    orch = Orchestrator()
    orch.register_workflow(wf)

    input_data = {
        "MapReduceStarter": [1,2,3,4,5,6]
    }

    orch.run_workflow("Demo3_MapReduce", input_data)
