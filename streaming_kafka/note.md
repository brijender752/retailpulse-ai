# Git Bash/MSYS: prefix container paths with `//` so MSYS does not translate
# `/opt/...` into a Windows host path. Docker/Linux treats `//opt/...` as
# `/opt/...` inside the container.
docker exec retailpulse-kafka //opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
