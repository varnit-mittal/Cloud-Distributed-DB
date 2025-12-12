#!/usr/bin/env bash
set -eu
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

echo "Starting ELK services (elasticsearch, kibana, logstash)..."
docker-compose up -d elasticsearch kibana logstash

echo "Waiting for Elasticsearch to be available at http://localhost:9200..."
for i in {1..60}; do
  if curl -sSf http://localhost:9200 >/dev/null 2>&1; then
    echo "Elasticsearch is up"
    break
  fi
  sleep 2
done

# Send sample JSON logs to Logstash TCP input (json_lines)
echo "Sending sample logs to Logstash on port 5000..."
for i in 1 2 3; do
  TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  PAYLOAD=$(printf '{"message":"test log %s","service":"test-service","level":"INFO","timestamp":"%s"}\n' "$i" "$TIMESTAMP")
  # try bash TCP first
  if exec 3<>/dev/tcp/localhost/5000 2>/dev/null; then
    printf "%s" "$PAYLOAD" >&3 || true
    exec 3>&-
  else
    # fallback to nc if available
    if command -v nc >/dev/null 2>&1; then
      printf "%s" "$PAYLOAD" | nc localhost 5000 || true
    else
      echo "Warning: could not send sample log (no /dev/tcp and no nc)"
    fi
  fi
  sleep 0.5
done

echo "Waiting a few seconds for Logstash to forward logs to Elasticsearch..."
sleep 5

echo "Searching Elasticsearch for the sample logs (docker-logs-*)..."
curl -s 'http://localhost:9200/docker-logs-*/_search?pretty' -H 'Content-Type: application/json' -d '{"query":{"match":{"message":"test log"}},"size":5}' || true
