Design and implement a POC for a workflow orchestration engine. Address issues with dynamic workflows, preserving workflow observability on a best-effort basis.

## Definitions

A __task__ is a unit of work - essentially a piece of code that runs on some inputs and produces some outputs. This code does not have to be pure; of particular interest here is the scenario where task code spawns other tasks. A __workflow__ is a collection of tasks, with dependencies between them. For example, a workflow can consist of two tasks, `A` and `B`, where the output of `A` is fed to `B`, creating the dependency `A -> B`. It is useful to think of a workflow as a directed graph, where tasks are nodes and dependencies between them are edges.

A workflow is __static__ if all of its tasks / dependencies are known ahead of time (before execution of the workflow starts). Likewise, a workflow is __dynamic__ if this is not true. For example, the case where workflow consists of a task A that spawns another task B on certain inputs is dynamic because the workflow orchestrator does not know about task B until task A starts executing. 

Of particular interest are two dynamic structures - map-reduce and branching. A Map Task is a task that dynamically spawns copies of itself to apply onto a list of inputs, then Reduces the results into a single output. Branching is when a task conditionally spawns other tasks based on a boolean expression computed using the inputs of the task. 

## Background:

Flyte is our current workflow execution engine. It is designed with the assumption that the workflow graph is known ahead of time. There are second-class escape mechanisms for implementing Map Tasks, Branching logic, and Dynamic workflows.

Because these are second-class elements, they do not reuse abstractions and machinery built for static workflows and re-implement much of it internally. Separate code paths need to be maintained to process these special workflow nodes. These nodes frequently have subtly different semantics and missing metadata.

Branching, in particular, is extremely unsatisfying because it supports a very limited set of comparison operations using an ad hoc DSL. Branching is decided on the workflow executor which computes the condition expression. This means that branch conditions need to be extremely primitive to avoid stalling the executor. Ideally we want to execute unrestricted Python conditions.

Dynamic workflows in Flyte are an after-thought. They are not adequately presented in the documentation, and their design is lacking. At best, they are a black-box that defines, registers, and executes another workflow internally at runtime. Again, this has different semantics and metadata from regular workflow execution and requires dedicated code paths.

We believe it is possible to create a workflow engine that gracefully adapts to workflows anywhere on the dynamic-to-static spectrum. **It should support workflows that decide the next task at the last possible moment in the preceding task. It should support workflows that are completely pre-defined ahead of time.** The exact same machinery should be used to define and run both types at the fundamental level.

We want to extract all possible benefits from the parts of the workflows execution plan that are known at any given time. The user should be informed of the upcoming actions as much as possible. One way to support this is to allow workflow developers to define constraints (level of parallelism, map-reduce, etc..) on tasks to inform the user beforehand as to how a given task can modify the execution graph.
Example constraints:
- No new nodes are created
- No new edges are created
- Only specific edges can be created
- Only specific nodes can be created
- No edges can be created starting at node A
- No edges can be created ending at node B

## Design Objectives:

- Prioritize the User Experience, then the Developer Experience. People running the workflow should get the best possible experience. Developers implementing the workflows should get the best possible experience, but not at the expense of the users
- Support fully static workflows using some kind of "Registration" phase
- Support fully dynamic workflows. E.g. task N+1 is decided by task N based on the time it runs
- Support in-between workflows by allowing the workflow to "Promise" (during Registration or during Runtime) a constraint. The Orchestrator must ensure a Promise is never broken (and terminate workflows that break Promises)
- Support a broad range of abstractions. Think about their implementation and UI. How will these be defined by the developer or detected by the workflow engine? How are they enforced (if I say I am a map task, could I be lying?)? Absolutely required are:
  - Map-reduce
  - Branching
- Record the actual execution history. After the workflow runs, all the steps taken by it should be displayed accurately
- Show best-effort visuals at any given time. E.g. as soon as we know task N+1, we must display it (even if we do not know task N+2). If accurately labeled, provisional visuals can be shown (e.g. the execution plan can change, but only in places explicitly labeled as TBD). Note: Displayed visuals must match the actual execution plan. If the workflow is not ready to commit, it should simply leave the execution plan unspecified

## Proof of Concept Objectives

Don't design yourself into a corner, the POC should be extendable to all the features you are trying to support. However, to save time, make sure to place strong restrictions on the "uninteresting" parts.

Suggested limitations:
- Run everything on one computer. Run subprocesses instead of containers
- No UI, just save the required metadata in some format. Some options: Graphviz, UML, matplotlib, JSON, CSV, LaTeX, HTML
- Store all data in-memory, do not worry about crash recovery of any kind
  - __Be ready to suggest databases/storage systems to use in production__
- Skip error reporting if it's inconvenient

## Proof of Concept Deliverables

At the end of the contracting period you should deliver code for the POC and give a presentation demoing how your POC works and detailing design choices and future work. We will ask questions.
