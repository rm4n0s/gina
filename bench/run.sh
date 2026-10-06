#!/usr/bin/env bash
# Gina vs net/http: throughput, latency and memory on one machine.
#
#   bench/run.sh                      # full matrix (about 10 minutes)
#   DURATION=2 RUNS=1 CORES="1 2" SCENARIOS=get bench/run.sh    # quick look
#
# The server is confined to physical cores 0..N-1 (taskset) and the load
# generator (oha) to a disjoint set of physical cores, so they never compete.
# Adjust SERVER_CPUS/CLIENT_CPUS for another machine (this one: 16 cores x 2 SMT,
# SMT sibling of CPU n is n+16, so cores 8-15 are CPUs 8-15,24-31).
set -u
cd "$(dirname "$0")/.."
DURATION=${DURATION:-5}      # seconds per measured run
RUNS=${RUNS:-3}              # measured runs per configuration (median is reported)
CONNS=${CONNS:-256}          # concurrent connections
CORES=${CORES:-"1 2 4 8"}
SCENARIOS=${SCENARIOS:-"get newconn echo64k"}
CLIENT_CPUS=${CLIENT_CPUS:-"8-15,24-31"}
SERVERS=${SERVERS:-"gina nethttp"}
TLS_MODES=${TLS_MODES:-"plain tls"}
OUT=${OUT:-bench/results.csv}
BIN=${BIN:-$(mktemp -d)}

go build -o "$BIN/gina" ./examples/httpserver || exit 1
(cd bench/nethttp && go build -o "$BIN/nethttp" .) || exit 1
head -c 65536 /dev/urandom > "$BIN/body64k.bin"
echo "scenario,server,tls,cores,run,rps,p50_ms,p99_ms,p999_ms,success,errors,rss_mb" > "$OUT"
port=21000

rss_mb() { # sum RSS of a process and its children
  local total=0 p
  for p in "$1" $(pgrep -P "$1"); do
    total=$((total + $(awk '/VmRSS/ {print $2}' /proc/$p/status 2>/dev/null || echo 0)))
  done
  echo $((total / 1024))
}

one() { # scenario server tls cores
  local scenario=$1 server=$2 tls=$3 cores=$4
  port=$((port + 1))
  local cpus="0-$((cores - 1))" scheme=http tlsflag="" ohaflags="--insecure --http-version 1.1" url path=/hello/bench
  [ "$tls" = tls ] && scheme=https && tlsflag="-tls"
  case $server in
    gina)    local w=""; [ "$cores" -gt 1 ] && w="-workers $cores"
             taskset -c "$cpus" "$BIN/gina" -port $port $w $tlsflag >/dev/null 2>&1 & ;;
    nethttp) GOMAXPROCS=$cores taskset -c "$cpus" "$BIN/nethttp" -port $port $tlsflag >/dev/null 2>&1 & ;;
  esac
  local pid=$!
  url="$scheme://127.0.0.1:$port"
  for _ in $(seq 100); do
    [ "$(curl -sk -o /dev/null -w '%{http_code}' "$url/hello/x" 2>/dev/null)" = 200 ] && break
    sleep 0.1
  done
  local extra="" conns=$CONNS
  case $scenario in
    newconn) extra="--disable-keepalive" ;;
    echo64k) path=/echo; extra="-m POST -D $BIN/body64k.bin -H Content-Type:application/octet-stream"; conns=64 ;;
  esac
  taskset -c "$CLIENT_CPUS" oha -z 1s -c "$conns" --no-tui $ohaflags $extra "$url$path" >/dev/null 2>&1 # warm-up
  local r
  for r in $(seq "$RUNS"); do
    taskset -c "$CLIENT_CPUS" oha -z "${DURATION}s" -c "$conns" --no-tui --output-format json $ohaflags $extra "$url$path" 2>/dev/null |
      python3 -c "
import json,sys
d=json.load(sys.stdin); s=d['summary']; p=d['latencyPercentiles']
ok=d['statusCodeDistribution'].get('200',0); errs=sum(v for k,v in (d.get('errorDistribution') or {}).items() if 'deadline' not in k)  # requests cut off at the deadline are not failures
print('$scenario,$server,$tls,$cores,$r,%.0f,%.3f,%.3f,%.3f,%.4f,%d,RSS' % (s['requestsPerSec'], p['p50']*1000, p['p99']*1000, p['p99.9']*1000, s['successRate'], errs))" |
      sed "s/RSS/$(rss_mb "$pid")/" >> "$OUT"
  done
  kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
  pkill -9 -P "$pid" 2>/dev/null
  tail -n "$RUNS" "$OUT" | awk -F, -v s="$scenario" -v v="$server" -v t="$tls" -v c="$cores" '{r[NR]=$6; l[NR]=$8; f+=$11} END {printf "  %-8s %-8s %-5s cores=%d  req/s=%s  p99=%sms  failed=%d\n", s, v, t, c, r[int((NR+1)/2)], l[int((NR+1)/2)], f}'
  sleep 0.5
}

for scenario in $SCENARIOS; do
  cores_list=$CORES
  [ "$scenario" = echo64k ] && cores_list=4
  for tls in $TLS_MODES; do
    for cores in $cores_list; do
      for server in $SERVERS; do one "$scenario" "$server" "$tls" "$cores"; done
    done
  done
done
echo "results: $OUT"
