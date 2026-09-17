from __future__ import annotations
import os, docker
from retailpulse.task_logging import log_execution
DBT_CONTAINER=os.getenv("RETAILPULSE_DBT_CONTAINER","retailpulse-dbt")
def run_dbt_command(command:str)->str:
    log_execution("/opt/retailpulse/dbt/dbt_project.yml", DBT_CONTAINER, command)
    c=docker.from_env().containers.get(DBT_CONTAINER)
    r=c.exec_run(["bash","-lc",f"cd /opt/retailpulse/dbt && {command}"],stdout=True,stderr=True)
    out=r.output.decode("utf-8",errors="replace")
    if r.exit_code!=0: raise RuntimeError(f"dbt command failed: {command}\n{out}")
    return out
