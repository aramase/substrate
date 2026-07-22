#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Reproducer for the counter demo's restore-to-useful-work benchmark.
#
# It repeatedly suspends an actor and then hits /work through the atenet router.
# The router auto-resumes the actor, so the request measures a cold restore
# followed by the guest touching its whole working set. The counter's /work
# response carries touch_ms, the in-guest time to fault + read the working set,
# which isolates the restore demand-fault cost from router/network latency.
#
# Prerequisites:
#   - The counter-microvm demo deployed with WORKING_SET_MIB > 0 (see README.md).
#   - The atenet router port-forwarded, e.g.:
#       kubectl port-forward -n ate-system svc/atenet-router 8000:80
#   - kubectl-ate on PATH.
#
# Usage:
#   demos/counter/bench-restore.sh [cycles]
#
# Configuration (environment variables and their defaults):
#   ACTOR=counter-bench      actor name (created if it does not exist)
#   ATESPACE=demo            atespace to run in (created if it does not exist)
#   TEMPLATE=ate-demo-counter-microvm/counter-microvm   template <ns>/<name>
#   CYCLES=10                number of suspend/resume measurement cycles
#   ENDPOINT=localhost:8000  router address reached by curl
#   KUBECTL_CONTEXT=         optional kube context for kubectl-ate

set -o errexit -o nounset -o pipefail

ACTOR="${ACTOR:-counter-bench}"
ATESPACE="${ATESPACE:-demo}"
TEMPLATE="${TEMPLATE:-ate-demo-counter-microvm/counter-microvm}"
CYCLES="${1:-${CYCLES:-10}}"
ENDPOINT="${ENDPOINT:-localhost:8000}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-}"

KATE=(kubectl-ate)
if [[ -n "${KUBECTL_CONTEXT}" ]]; then
    KATE+=(--context "${KUBECTL_CONTEXT}")
fi
HOST="${ACTOR}.${ATESPACE}.actors.resources.substrate.ate.dev"

for tool in kubectl-ate curl; do
    command -v "${tool}" >/dev/null 2>&1 || {
        echo "error: '${tool}' is required but was not found on PATH" >&2
        exit 1
    }
done

status() {
    "${KATE[@]}" get actor "${ACTOR}" --atespace "${ATESPACE}" 2>/dev/null |
        awk -v a="${ACTOR}" '$2==a {print $4}'
}

wait_status() {
    local want="$1" i
    for ((i = 0; i < 60; i++)); do
        [[ "$(status)" == "${want}" ]] && return 0
        sleep 1
    done
    echo "error: timed out waiting for ${ACTOR} to reach ${want}" >&2
    return 1
}

# work hits /work through the router; the router auto-resumes a suspended actor,
# so this returns once the guest is running and has served the request. It prints
# the touch_ms value from the response, or nothing on failure.
work() {
    curl -s -m 120 -H "Host: ${HOST}" "http://${ENDPOINT}/work" |
        sed -n 's/.*touch_ms=\([0-9.]*\).*/\1/p'
}

"${KATE[@]}" create atespace "${ATESPACE}" >/dev/null 2>&1 || true
if ! "${KATE[@]}" get actor "${ACTOR}" --atespace "${ATESPACE}" >/dev/null 2>&1; then
    "${KATE[@]}" create actor "${ACTOR}" --atespace "${ATESPACE}" -t "${TEMPLATE}" >/dev/null
fi

echo ">> Warming ${ACTOR} (resume + first /work, discarded)..." >&2
warm="$(work)"
if [[ -z "${warm}" ]]; then
    echo "error: /work returned no touch_ms. Is WORKING_SET_MIB > 0 and the router" >&2
    echo "       port-forwarded to ${ENDPOINT}?" >&2
    exit 1
fi

echo ">> Measuring ${CYCLES} suspend/resume cycles..." >&2
echo "cycle,touch_ms"
samples=()
for ((c = 1; c <= CYCLES; c++)); do
    "${KATE[@]}" suspend actor "${ACTOR}" --atespace "${ATESPACE}" >/dev/null 2>&1
    wait_status STATUS_SUSPENDED
    ms="$(work)"
    echo "${c},${ms:-FAIL}"
    [[ -n "${ms}" ]] && samples+=("${ms}")
done

if ((${#samples[@]} > 0)); then
    printf '%s\n' "${samples[@]}" | sort -n | awk '
    { v[NR] = $1; sum += $1 }
    END {
      n = NR
      p50 = v[int((n + 1) / 2)]
      p95 = v[int((0.95 * n) + 0.5)]
      if (p95 == "") p95 = v[n]
      printf ">> restore-to-useful-work (touch_ms): n=%d min=%.1f p50=%.1f p95=%.1f max=%.1f avg=%.1f\n",
        n, v[1], p50, p95, v[n], sum / n
    }' >&2
fi
