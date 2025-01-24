from src.orchestrator import Orchestrator
from src.constraints import TaskConstraints
from src.workflow import Task, Workflow

GLOBAL_TASKS = {}

###############################################################################
# 1. Developer: Create Task Funcs
###############################################################################

def map_reduce_starter_func(wf_id, task_id, inputs, orchestrator):
    """
    Spawns map task on each input and adds it as a dependency for ReduceTask
    """
    print(f"[map_reduce_starter_func] {task_id} sees inputs={inputs}")
    
    all_map_tids = []
    for i, val in enumerate(inputs):
        map_tid = f"{task_id}_Map_{i+1}"
        all_map_tids.append(map_tid)
        map_constraints = TaskConstraints()
        map_task = Task(map_tid, map_func, constraints=map_constraints)
        orchestrator.spawn_task(
            wf_id=wf_id,
            creator_task_id=task_id,
            new_task=map_task,
            input_data=val
        )

    reduce_tid = f"{task_id}_Reduce"
    reduce_valid_previous_nodes = all_map_tids + [task_id]
    reduce_constraints = TaskConstraints(
        valid_previous_nodes_policy='custom',
        valid_previous_nodes=reduce_valid_previous_nodes, # the reduce task can have mapreduce and map nodes be able to interact with it
        valid_incoming_edges_policy='custom',
        valid_incoming_edges=[(m, reduce_tid) for m in all_map_tids]
    )
    reduce_task = Task(reduce_tid, reduce_func, constraints=reduce_constraints)
    orchestrator.spawn_task(
        wf_id=wf_id,
        creator_task_id=task_id,
        new_task=reduce_task,
        new_edges=[(m, reduce_tid) for m in all_map_tids], # link the earlier spawned map tasks to reduce task to make the dependency
    )
    print(f"[map_reduce_starter_func] {task_id} finished")
    return None

def map_func(wf_id, task_id, inputs, orchestrator):
    """
    Doubles the input
    """
    print(f"[map_func] {task_id} sees inputs={inputs}")
    result = sum(inputs) if isinstance(inputs, list) else inputs
    result *= 2
    print(f"[map_func] {task_id} => {result}")
    return result

def reduce_func(wf_id, task_id, inputs, orchestrator):
    """
    Summation of the partial results
    """
    print(f"[reduce_func] {task_id} sees {inputs}")
    result = sum(inputs) if isinstance(inputs, list) else inputs
    orchestrator.spawn_task(wf_id=wf_id, creator_task_id=task_id, new_task=GLOBAL_TASKS['StaticTask2'], new_edges=[(task_id, 'StaticTask2')], skip_visual_edge=True)
    print(f"[reduce_func] {task_id} => {inputs}")
    return result

def branch_func(wf_id, task_id, inputs, orchestrator):
    """
    If input > 10 => spawn BranchA, else BranchB
    """
    global GLOBAL_TASKS
    print(f"[branch_func] {task_id} input={inputs}")
    sum_inputs = sum(inputs) if isinstance(inputs, list) else inputs
    print(f"[branch_func] sum_inputs={sum_inputs}")
    if sum_inputs > 100:
        print(f"[branch_func] Spawning BranchA")
        orchestrator.spawn_task(wf_id=wf_id, creator_task_id=task_id, new_task=GLOBAL_TASKS['BranchA'], input_data=inputs)
    elif sum_inputs > 50:
        print(f"[branch_func] Spawning BranchB")
        orchestrator.spawn_task(wf_id=wf_id, creator_task_id=task_id, new_task=GLOBAL_TASKS['BranchB'], input_data=inputs)
    else:
        print(f"[branch_func] Spawning BranchB")
        orchestrator.spawn_task(wf_id=wf_id, creator_task_id=task_id, new_task=GLOBAL_TASKS['BranchB'], input_data=inputs)

    print(f"[branch_func] exiting")
    return None

def branch_a_func(wf_id, task_id, inputs, orchestrator):
    """
    If input > 10 => spawn BranchA, else BranchB
    """
    global GLOBAL_TASKS
    print(f"[branch_a_func] {task_id} input={inputs}")
    sum_inputs = sum(inputs) if isinstance(inputs, list) else inputs
    print(f"[branch_a_func] sum_inputs={sum_inputs}")
    if sum_inputs > 100:
        orchestrator.spawn_task(wf_id=wf_id, creator_task_id=task_id, new_task=GLOBAL_TASKS['MapReduceStarter'], input_data=[sum_inputs + (i*10) for i in range(5)])
    else:
        orchestrator.spawn_task(wf_id=wf_id, creator_task_id=task_id, new_task=GLOBAL_TASKS['BranchD'], input_data=[sum_inputs])
    
    print(f"[branch_a_func] exiting")
    return None

def static_func1(wf_id, task_id, inputs, orchestrator):
    print(f"[static_func1] {task_id} input={inputs}")
    result = sum(inputs) if isinstance(inputs, list) else inputs
    result += 100
    print(f"[static_func1] {task_id} => {result}")
    return result

def static_func2(wf_id, task_id, inputs, orchestrator):
    print(f"[static_func2] {task_id} input={inputs}")
    result = sum(inputs) if isinstance(inputs, list) else inputs
    result += 5000
    print(f"[static_func2] {task_id} => {result}")
    return result

###############################################################################
# 2. Developer: Create Tasks + Workflow
###############################################################################

def build_demo_workflow():
    # 1. creating the tasks
    global GLOBAL_TASKS

    # Some static task
    static_task1 = Task("StaticTask1", static_func1)
    GLOBAL_TASKS['StaticTask1'] = static_task1

    static_task2 = Task("StaticTask2", static_func2)
    GLOBAL_TASKS['StaticTask2'] = static_task2

    # Branching Related Tasks
    branch_a_constraints = TaskConstraints(
        valid_next_nodes_policy='custom',
        valid_next_nodes=['MapReduceStarter', 'BranchD'],
        valid_outgoing_edges_policy='custom',
        valid_outgoing_edges=[('BranchA', 'MapReduceStarter'), ('BranchA', 'BranchD')],
    )
    branch_a = Task("BranchA", branch_a_func, constraints=branch_a_constraints)
    GLOBAL_TASKS['BranchA'] = branch_a
    
    branch_b = Task("BranchB", static_func2)
    GLOBAL_TASKS['BranchB'] = branch_b

    branch_c = Task("BranchC", static_func2)
    GLOBAL_TASKS['BranchC'] = branch_c

    branch_d = Task("BranchD", static_func2)
    GLOBAL_TASKS['BranchD'] = branch_d

    branching_node_constraints = TaskConstraints(
        max_spawn_count=1,  # can spawn only one task because it is a branch node
        valid_next_nodes_policy='custom',
        valid_next_nodes=['BranchA', 'BranchB', 'BranchC'],
        valid_outgoing_edges_policy='custom',
        valid_outgoing_edges=[('BranchTask', 'BranchA'), ('BranchTask', 'BranchB'), ('BranchTask', 'BranchC')],
    )
    branch_task = Task("BranchTask", func=branch_func, constraints=branching_node_constraints)
    GLOBAL_TASKS['BranchTask'] = branch_task

    # Map Reduce Starter Task
    map_reduce_start_spawn_count = 10
    map_task_ids1 = [f'MapReduceStarter_Map_{i}' for i in range(1, map_reduce_start_spawn_count + 1)]
    reduce_task_ids1 = [f'MapReduceStarter_Reduce']
    map_reduce_starter_valid_next_nodes = map_task_ids1 + reduce_task_ids1
    map_reduce_starter_constraints1 = TaskConstraints(
        max_spawn_count=map_reduce_start_spawn_count,
        valid_next_nodes_policy='custom',
        valid_next_nodes=map_reduce_starter_valid_next_nodes
    )
    map_reduce_starter_task = Task("MapReduceStarter", func=map_reduce_starter_func, constraints=map_reduce_starter_constraints1, metadata={'type': 'map_reduce_starter'})
    GLOBAL_TASKS['MapReduceStarter'] = map_reduce_starter_task

    # 2. Build the workflow
    wf = Workflow("Demo5")
    wf.add_task(static_task1)
    wf.add_task(branch_task, dependencies=['StaticTask1'])

    return wf

###############################################################################
# 3. User: Run the Workflow with Input Data
###############################################################################
if __name__=="__main__":
    wf = build_demo_workflow()

    orch = Orchestrator()
    orch.register_workflow(wf)

    input_data = {
        "StaticTask1": [1,2,3,4]
    }

    orch.run_workflow("Demo5", input_data)
