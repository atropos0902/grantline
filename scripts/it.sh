#!/usr/bin/env bash
# Runs the integration tests without the container library's chatter.
set -o pipefail
go test ./test/integration -count=1 "$@" 2>&1 | grep -vE "🐳|🔔|✅|🚫|⏳|testcontainers|Server Version|API Version|Operating|Total Memory|Labels:|Test Session|Test Process|com.docker"
