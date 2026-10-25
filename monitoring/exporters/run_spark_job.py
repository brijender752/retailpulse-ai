"""Run a Spark application and atomically persist its latest execution metrics."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid


def main():
    script = Path(sys.argv[1])
    directory = Path('/opt/monitoring-state/jobs')
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / (script.stem + '.json')
    started = time.time()

    def publish(**values):
        temporary = directory / (uuid.uuid4().hex + '.tmp')
        temporary.write_text(json.dumps(dict(job=script.stem, started=started, **values)))
        os.replace(temporary, target)

    publish(running=1)
    try:
        result = subprocess.run(['/opt/spark/bin/spark-submit', str(script), *sys.argv[2:]])
        code = result.returncode
    except Exception:
        publish(running=0, success=0, duration=time.time() - started, completed=time.time())
        raise
    publish(running=0, success=int(code == 0), duration=time.time() - started, completed=time.time())
    return code


if __name__ == '__main__':
    sys.exit(main())
