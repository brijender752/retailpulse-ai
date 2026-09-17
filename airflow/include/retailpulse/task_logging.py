"""Task provenance headers without logging arguments or credentials."""
import inspect
import logging


def log_task_start(context):
    task = context["task"]
    logger = logging.getLogger(__name__)
    logger.info("=== TASK EXECUTION: DAG=%s | TASK=%s ===", task.dag_id, task.task_id)
    logger.info("DAG file: %s", task.dag.fileloc)
    logger.info("Operator: %s", type(task).__name__)
    function = getattr(task, "python_callable", None)
    if function:
        logger.info("Python callable: %s.%s | Script: %s",
                    function.__module__, function.__qualname__, inspect.getsourcefile(function))
    target = getattr(task, "trigger_dag_id", None)
    if target:
        logger.info("Triggers DAG: %s", target)


def log_execution(script, container=None, command=None):
    logger = logging.getLogger(__name__)
    logger.info("Execution script/module: %s", script)
    if container:
        logger.info("Execution container: %s", container)
    if command:
        logger.info("Execution command: %s", command)
