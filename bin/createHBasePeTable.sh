#!/usr/bin/env bash

set -euo pipefail

DEFAULT_FAMILY="info0"
DEFAULT_CLIENTS="1"

tableName=""
rowCount=""
familyName="${DEFAULT_FAMILY}"
clientCount="${DEFAULT_CLIENTS}"
dryRun=false
namespace=""
HBASE_BIN=""

usage() {
    cat <<'EOF'
Usage:
    createHBasePeTable.sh --table <tab|ns:tab> --rows <rows> [OPTIONS]

Options:
    --table <tab|ns:tab>    HBase table name. Required.
    --rows <rows>           Rows to append. Required, positive integer.
    --clients <n>           hbase pe client count. Default: 1.
    --dry-run               Print commands without executing them.
    -h, --help              Show this help.

Examples:
    createHBasePeTable.sh --table manga:peTest --rows 10000 --clients 4
EOF
}

die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

isPositiveInteger() {
    [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

validateNamePart() {
    local value="$1"
    local label="$2"

    [[ -n "${value}" ]] || die "${label} must not be empty"
    [[ "${value}" != *$'\n'* ]] || die "${label} must not contain newline"
    [[ "${value}" != *"'"* ]] || die "${label} must not contain single quote"
}

quoteForShell() {
    printf '%q' "$1"
}

printCommand() {
    local -a command=("$@")
    local rendered=""
    local arg

    for arg in "${command[@]}"; do
        if [[ -n "${rendered}" ]]; then
            rendered+=" "
        fi
        rendered+="$(quoteForShell "${arg}")"
    done

    printf '%s\n' "${rendered}"
}

printHBaseShellCommand() {
    local command="$1"
    local escapedCommand="${command}"
    local escapedHBaseBin="${HBASE_BIN}"

    escapedCommand="${escapedCommand//\\/\\\\}"
    escapedCommand="${escapedCommand//\"/\\\"}"
    escapedCommand="${escapedCommand//\$/\\\$}"
    escapedCommand="${escapedCommand//\`/\\\`}"

    escapedHBaseBin="${escapedHBaseBin//\\/\\\\}"
    escapedHBaseBin="${escapedHBaseBin//\"/\\\"}"
    escapedHBaseBin="${escapedHBaseBin//\$/\\\$}"
    escapedHBaseBin="${escapedHBaseBin//\`/\\\`}"

    printf 'echo "%s" | "%s" shell\n' "${escapedCommand}" "${escapedHBaseBin}"
}

runHBaseShellCommand() {
    local command="$1"

    if [[ "${dryRun}" == true ]]; then
        printHBaseShellCommand "${command}"
        return 0
    fi

    printf '%s\n' "${command}" | "${HBASE_BIN}" shell
}

parseArgs() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --table)
                [[ $# -ge 2 ]] || die "--table requires a value"
                tableName="$2"
                shift 2
                ;;
            --rows)
                [[ $# -ge 2 ]] || die "--rows requires a value"
                rowCount="$2"
                shift 2
                ;;
            --clients)
                [[ $# -ge 2 ]] || die "--clients requires a value"
                clientCount="$2"
                shift 2
                ;;
            --dry-run)
                dryRun=true
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                die "unknown argument: $1"
                ;;
        esac
    done
}

validateArgs() {
    [[ -n "${tableName}" ]] || die "--table is required"
    [[ -n "${rowCount}" ]] || die "--rows is required"

    isPositiveInteger "${rowCount}" || die "--rows must be a positive integer"
    isPositiveInteger "${clientCount}" || die "--clients must be a positive integer"

    validateNamePart "${tableName}" "table name"
    validateNamePart "${familyName}" "family name"

    [[ "${tableName}" != *:*:* ]] || die "--table must be 'tab' or 'ns:tab'"

    if [[ "${tableName}" == *":"* ]]; then
        namespace="${tableName%%:*}"
        local tableOnly="${tableName#*:}"
        validateNamePart "${namespace}" "namespace"
        validateNamePart "${tableOnly}" "table name"
    else
        namespace=""
    fi

    [[ -n "${HBASE_HOME:-}" ]] || die "HBASE_HOME is not set"
    [[ -d "${HBASE_HOME}" ]] || die "HBASE_HOME does not exist: ${HBASE_HOME}"

    HBASE_BIN="${HBASE_HOME}/bin/hbase"
    [[ -x "${HBASE_BIN}" ]] || die "hbase executable not found: ${HBASE_BIN}"
}

tableExists() {
    local output="$1"

    if printf '%s\n' "${output}" | grep -Fq "Table ${tableName} does exist"; then
        return 0
    fi

    if printf '%s\n' "${output}" | grep -Fq "Table ${tableName} does not exist"; then
        return 1
    fi

    die "failed to parse table existence from hbase shell output"
}

prepareTable() {
    local existsCommand="exists '${tableName}'"
    local createCommand="create '${tableName}', {NAME => '${familyName}', REPLICATION_SCOPE => 1}"
    local alterCommand="alter '${tableName}', {NAME => '${familyName}', REPLICATION_SCOPE => 1}"

    if [[ "${dryRun}" == true ]]; then
        printHBaseShellCommand "${existsCommand}"
        printf '# if table does not exist:\n'
        printHBaseShellCommand "${createCommand}"
        printf '# if table exists:\n'
        printHBaseShellCommand "${alterCommand}"
        return 0
    fi

    local existsOutput
    existsOutput="$(runHBaseShellCommand "${existsCommand}")"

    if tableExists "${existsOutput}"; then
        runHBaseShellCommand "${alterCommand}"
    else
        runHBaseShellCommand "${createCommand}"
    fi
}

extractRowCount() {
    local output="$1"
    local countLine

    countLine="$(printf '%s\n' "${output}" | awk '/^[0-9]+ row\(s\)$/ { value = $1 } END { print value }')"
    [[ -n "${countLine}" ]] || die "failed to parse current row count from hbase shell output"

    printf '%s\n' "${countLine}"
}

main() {
    parseArgs "$@"
    validateArgs

    prepareTable

    local currentRows="0"
    if [[ "${dryRun}" == true ]]; then
        printHBaseShellCommand "count '${tableName}', INTERVAL => 1000000"
        printf '# dry-run assumes current row count is 0 for --startRow preview\n' >&2
    else
        local countOutput
        countOutput="$(runHBaseShellCommand "count '${tableName}', INTERVAL => 1000000")"
        currentRows="$(extractRowCount "${countOutput}")"
    fi

    local -a peCommand=(
        "${HBASE_BIN}"
        pe
        --nomapred
        "--table=${tableName}"
        "--rows=${rowCount}"
        "--startRow=${currentRows}"
        sequentialWrite
        "${clientCount}"
    )

    if [[ "${dryRun}" == true ]]; then
        printCommand "${peCommand[@]}"
    else
        "${peCommand[@]}"
    fi
}

main "$@"
