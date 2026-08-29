#!/usr/bin/env bash

set -u

: "${AWS_REGION:?AWS_REGION is required}"
: "${SSM_TARGET:?SSM_TARGET is required}"
: "${DWH_HOST:?DWH_HOST is required}"
: "${DWH_PORT:?DWH_PORT is required}"

SSM_LOCAL_PORT="${SSM_LOCAL_PORT:-15432}"

echo "[$(date -u +%FT%TZ)] starting socat relay 0.0.0.0:${DWH_PORT} -> 127.0.0.1:${SSM_LOCAL_PORT}"
socat TCP-LISTEN:"${DWH_PORT}",fork,reuseaddr TCP:127.0.0.1:"${SSM_LOCAL_PORT}" &

PROFILE_ARGS=()
if [[ -n "${AWS_PROFILE:-}" ]]; then
    PROFILE_ARGS=(--profile "${AWS_PROFILE}")
fi

while true; do
    echo "[$(date -u +%FT%TZ)] starting SSM port-forwarding session to ${DWH_HOST}:${DWH_PORT} (local port ${SSM_LOCAL_PORT})..."
    aws ssm start-session \
        --target "${SSM_TARGET}" \
        --document-name AWS-StartPortForwardingSessionToRemoteHost \
        --region "${AWS_REGION}" \
        "${PROFILE_ARGS[@]}" \
        --parameters "{\"host\":[\"${DWH_HOST}\"],\"portNumber\":[\"${DWH_PORT}\"],\"localPortNumber\":[\"${SSM_LOCAL_PORT}\"]}"
    exit_code=$?
    echo "[$(date -u +%FT%TZ)] session ended (exit code ${exit_code}) -- reconnecting in 2s..."
    sleep 2
done
