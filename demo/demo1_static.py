from src.orchestrator import Orchestrator
from src.constraints import TaskConstraints
from src.workflow import Task, Workflow

GLOBAL_TASKS = {}

###############################################################################
# 1. Developer: Create Task Funcs
###############################################################################

def static_add_100(wf_id, task_id, inputs, orchestrator):
    result = sum(inputs) if isinstance(inputs, list) else inputs
    result += 100
    return result

def static_add_500(wf_id, task_id, inputs, orchestrator):
    result = sum(inputs) if isinstance(inputs, list) else inputs
    result += 500
    return result

def static_add_2000(wf_id, task_id, inputs, orchestrator):
    result = sum(inputs) if isinstance(inputs, list) else inputs
    result += 2000
    return result

###############################################################################
# 2. Developer: Create Tasks + Workflow
###############################################################################

def build_demo_workflow():
    # 2.1 Creating the tasks
    global GLOBAL_TASKS

    # Some static task
    static_task1 = Task("StaticTask1", static_add_100)
    GLOBAL_TASKS['StaticTask1'] = static_task1

    static_task2 = Task("StaticTask2", static_add_100)
    GLOBAL_TASKS['StaticTask2'] = static_task2

    static_task3 = Task("StaticTask3", static_add_500)
    GLOBAL_TASKS['StaticTask3'] = static_task3

    static_task4 = Task("StaticTask4", static_add_500)
    GLOBAL_TASKS['StaticTask4'] = static_task4

    static_task5 = Task("StaticTask5", static_add_2000)
    GLOBAL_TASKS['StaticTask5'] = static_task5

    static_task6 = Task("StaticTask6", static_add_2000)
    GLOBAL_TASKS['StaticTask6'] = static_task6

    static_task7 = Task("StaticTask7", static_add_2000)
    GLOBAL_TASKS['StaticTask7'] = static_task7

    # 2.2 Build the workflow
    wf = Workflow("Demo1_Static")
    wf.add_task(static_task1)
    wf.add_task(static_task2)
    wf.add_task(static_task3, dependencies=['StaticTask1'])
    wf.add_task(static_task4, dependencies=['StaticTask2'])
    wf.add_task(static_task5, dependencies=['StaticTask2'])
    wf.add_task(static_task6, dependencies=['StaticTask4'])
    wf.add_task(static_task7, dependencies=['StaticTask6'])

# start with two nodes, first one will have one more nodes, second one will have two more nodes, and add any two more nodes in a linear manner


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

    orch.run_workflow("Demo1_Static", input_data)
