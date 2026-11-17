#!/usr/bin/env bash
# Starts 3 limiter nodes on :8081-8083 sharing a local Redis, then sends requests round-robin.
# Prereqs: `mvn package` has been run and redis-server is listening on localhost:6379.
set -euo pipefail
cd "$(dirname "$0")/.."
JAR=target/distributed-rate-limiter.jar
[ -f "$JAR" ] || { echo "build first: mvn -q package -DskipTests"; exit 1; }
redis-cli ping >/dev/null || { echo "start redis first: redis-server"; exit 1; }

pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT

for port in 8081 8082 8083; do
  RL_MODE=REDIS RL_SERVER_PORT=$port RL_FAILURE_EXPECTED_NODES=3 java -jar "$JAR" > "/tmp/rl-$port.log" 2>&1 &
  pids+=($!)
done
for port in 8081 8082 8083; do
  until curl -sf "localhost:$port/healthz" >/dev/null; do sleep 0.2; done
done
redis-cli --scan --pattern 'rl:demo:*' | xargs -r redis-cli del >/dev/null

echo "Rule 'demo': burst 5, refill 1/s. 10 requests for client 'alice', round-robin over 3 nodes:"
for i in $(seq 0 9); do
  port=$((8081 + i % 3))
  code=$(curl -s -o /dev/null -w "%{http_code}" -H "X-Client-Id: alice" "localhost:$port/demo/hello")
  echo "  request $((i + 1)) -> node :$port -> HTTP $code"
done
echo
echo "Exactly 5 succeed in total: the nodes share one bucket through Redis."
