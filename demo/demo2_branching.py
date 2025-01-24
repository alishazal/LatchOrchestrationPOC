from src.orchestrator import Orchestrator
from src.constraints import TaskConstraints
from src.workflow import Task, Workflow

GLOBAL_TASKS = {}

###############################################################################
# 1. Developer: Create Task Funcs
###############################################################################

def branch_func(wf_id, task_id, inputs, orchestrator):
    global GLOBAL_TASKS
    sum_inputs = sum(inputs) if isinstance(inputs, list) else inputs
    if sum_inputs > 100:
        orchestrator.spawn_task(wf_id=wf_id, creator_task_id=task_id, new_task=GLOBAL_TASKS['BranchA'], input_data=inputs)
    else:
        orchestrator.spawn_task(wf_id=wf_id, creator_task_id=task_id, new_task=GLOBAL_TASKS['BranchB'], input_data=inputs)

def static_add_100(wf_id, task_id, inputs, orchestrator):
    result = sum(inputs) if isinstance(inputs, list) else inputs
    result += 100
    return result

def static_multiply_by_2(wf_id, task_id, inputs, orchestrator):
    result = sum(inputs) if isinstance(inputs, list) else inputs
    result *= 2
    return result

###############################################################################
# 2. Developer: Create Tasks + Workflow
###############################################################################

def build_demo_workflow():
    global GLOBAL_TASKS

    # 2.1 Creating the tasks
    branch_a = Task("BranchA", static_add_100)
    GLOBAL_TASKS['BranchA'] = branch_a
    
    branch_b = Task("BranchB", static_multiply_by_2)
    GLOBAL_TASKS['BranchB'] = branch_b

    branching_node_constraints = TaskConstraints(
        max_spawn_count=1,  # can spawn only one task because it is a branch node
        valid_next_nodes_policy='custom',
        valid_next_nodes=['BranchA', 'BranchB'],
        valid_outgoing_edges_policy='custom',
        valid_outgoing_edges=[('BranchTask', 'BranchA'), ('BranchTask', 'BranchB')],
    )

    branch_task = Task("BranchTask", func=branch_func, constraints=branching_node_constraints)
    GLOBAL_TASKS['BranchTask'] = branch_task

    # 2.2 Build the workflow
    wf = Workflow("Demo2_Branching")
    wf.add_task(branch_task)

    return wf

###############################################################################
# 3. User: Run the Workflow with Input Data
###############################################################################
if __name__=="__main__":
    wf = build_demo_workflow()

    orch = Orchestrator()
    orch.register_workflow(wf)

    input_data = {
        "BranchTask": [1,2,3,4]
    }

    orch.run_workflow("Demo2_Branching", input_data)
