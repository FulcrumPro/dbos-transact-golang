#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

if [[ -z "${DBOS_SYSTEM_DATABASE_URL:-}" ]]; then
	go run ./cmd/dbos postgres start
fi

test_args=("$@")
if ((${#test_args[@]} == 0)); then
	test_args=(./...)
fi

has_timeout=false
for arg in "${test_args[@]}"; do
	case "$arg" in
	-timeout|-timeout=*)
		has_timeout=true
		break
		;;
	esac
done

if [[ "$has_timeout" == false ]]; then
	test_args=(-timeout "${DBOS_TEST_TIMEOUT:-30m}" "${test_args[@]}")
fi

go test "${test_args[@]}"
