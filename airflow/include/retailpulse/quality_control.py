import os, docker
QUALITY_CONTAINER = os.getenv("RETAILPULSE_QUALITY_CONTAINER","retailpulse-quality")

def run_quality_job(path: str) -> str:
    c = docker.from_env().containers.get(QUALITY_CONTAINER)
    cmd = f"spark-submit /opt/retailpulse/{path}"
    r = c.exec_run(["bash","-lc",cmd], stdout=True, stderr=True)
    out = r.output.decode("utf-8", errors="replace")
    if r.exit_code != 0:
        raise RuntimeError(f"Quality job failed\n{out}")
    return out
