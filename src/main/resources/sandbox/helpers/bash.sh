#!/usr/bin/env bash
# StudioQuack in-sandbox Bash helper. Prepended to the user's node script by
# GenerateScriptService; the "__SQ_CONTEXT__" token is replaced with the Base64 execution context.
# Keep this template free of printf-style %s placeholders.
_SQ_CTX_ENCODED="__SQ_CONTEXT__"
_SQ_CTX_JSON="$(echo "$_SQ_CTX_ENCODED" | base64 -d 2>/dev/null || echo '{}')"
export SQ_CONTEXT_JSON="$_SQ_CTX_JSON"

# sq_full_context — echo the raw decoded execution context JSON.
sq_full_context() { echo "$SQ_CONTEXT_JSON"; }

# _sq_json_escape <text> — escape a value so it is safe inside a JSON string literal. Backslash is
# replaced FIRST, or it would double-escape the backslashes the later rules introduce.
_sq_json_escape() {
  local _sq_e="$1"
  _sq_e=${_sq_e//\\/\\\\}
  _sq_e=${_sq_e//\"/\\\"}
  _sq_e=${_sq_e//$'\n'/\\n}
  _sq_e=${_sq_e//$'\r'/\\r}
  _sq_e=${_sq_e//$'\t'/\\t}
  echo "$_sq_e"
}

# _sq_emit <json-object> — write one structured event marker the daemon parses off stdout.
_sq_emit() { echo "sqEvent:::$1"; }

# sq_log <message> [level]
sq_log() {
  _sq_emit "{\"kind\":\"log\",\"level\":\"$(_sq_json_escape "${2:-info}")\",\"message\":\"$(_sq_json_escape "$1")\"}"
}

# sq_metric <name> <value> — value is emitted raw so it stays a JSON number.
sq_metric() {
  _sq_emit "{\"kind\":\"metric\",\"name\":\"$(_sq_json_escape "$1")\",\"value\":$2}"
}

# sq_return_result <port> <value> — record one per-port output. This marker is the authoritative
# result channel and its shape is parsed by the daemon; do not change it.
sq_return_result() {
  echo "portValue:$1:::result:$2"
}

# A failing bash node must NOT print an error as a port value — the next node would consume it as
# a message. Because bash is emit-only, every event below is delivered when the process exits — an
# escalation/signal/message cannot fire "mid-step" from bash.

# sq_fail <errorCode> [message] — typed business error, then exit 1 so no later port marker is emitted.
# "SQ_SKIP" is reserved for the monitoring skip action and is refused.
sq_fail() {
  local _sq_code="$1" _sq_msg="${2:-}"
  if [ -z "$_sq_code" ] || [ "$_sq_code" = "SQ_SKIP" ]; then
    echo "sq_fail: errorCode is required and SQ_SKIP is reserved" >&2
    exit 1
  fi
  _sq_emit "{\"kind\":\"fail\",\"errorCode\":\"$(_sq_json_escape "$_sq_code")\",\"message\":\"$(_sq_json_escape "$_sq_msg")\"}"
  echo "SQ_FAIL $_sq_code: $_sq_msg" >&2
  exit 1
}

# sq_escalate <escalationCode> [message] — throw an escalation caught by an Escalation Boundary Event
# with the same code on this node (delivered at process exit on this runtime).
sq_escalate() {
  _sq_emit "{\"kind\":\"escalate\",\"escalationCode\":\"$(_sq_json_escape "$1")\",\"message\":\"$(_sq_json_escape "${2:-}")\"}"
}

# sq_send_signal <signalName> [scope] — broadcast a BPMN signal (scope: global|instance). Names under
# the reserved "__sq_" prefix are engine-internal and refused.
sq_send_signal() {
  case "$1" in __sq_*) echo "sq_send_signal: reserved signal name" >&2; return 1;; esac
  _sq_emit "{\"kind\":\"signal\",\"signalName\":\"$(_sq_json_escape "$1")\",\"scope\":\"$(_sq_json_escape "${2:-global}")\"}"
}

# sq_send_message <messageName> [correlationKey] — correlate a BPMN message (default: this run).
sq_send_message() {
  _sq_emit "{\"kind\":\"message\",\"messageName\":\"$(_sq_json_escape "$1")\",\"correlationKey\":\"$(_sq_json_escape "${2:-}")\"}"
}

# Interrupt cooperation: when an interrupting boundary (timer/message/signal/escalation) fires, the
# daemon sends SIGTERM and, after a short grace, SIGKILL. Register cleanup with sq_on_cancel; poll
# sq_is_cancelled inside long loops.
_SQ_CANCELLED=0
_SQ_CANCEL_HOOKS=()
_sq_on_term() {
  _SQ_CANCELLED=1
  local _sq_hook
  for _sq_hook in ${_SQ_CANCEL_HOOKS[@]+"${_SQ_CANCEL_HOOKS[@]}"}; do "$_sq_hook" || true; done
  exit 143
}
trap _sq_on_term TERM
# sq_on_cancel <function-name> — run the function once when the step is interrupted.
sq_on_cancel() { _SQ_CANCEL_HOOKS+=("$1"); }
# sq_is_cancelled — exit status 0 once an interrupt was observed.
sq_is_cancelled() { [ "$_SQ_CANCELLED" = "1" ]; }
