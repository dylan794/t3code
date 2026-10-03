# Jarvis memory boundary

The Pi adapter launches the Jarvis extension with a host identity derived from
the configured `JARVIS_T3_ENVIRONMENT_ID`, provider instance and thread. Set a
different stable environment ID for each server profile and device. This is
an identity claim, not an Owner credential or permission to recall history.
Missing or malformed IDs clear the child process's inherited binding fields.

The child receives `JARVIS_T3_HOST_CONNECTION_ID`,
`JARVIS_T3_ENVIRONMENT_ID` and `JARVIS_T3_THREAD_ID`. An Owner must separately
enroll the exact binding in Jarvis's authenticated controls. Jarvis verifies
the current Owner Session and the actual Pi provider/model/API endpoint before
lookup and again before accepting a disposable recall packet. T3 does not
infer the endpoint from a model slug and does not prepend memory to a saved
prompt. Model changes do not change host identity and must pass a new exact
destination check.

## Private unlock is unavailable

This adapter rejects `secret_input` and acknowledgment-bearing
`set_owner_thread_concealed` requests. It replies with cancellation and
`concealed: false`; no password question or private request title reaches the
ordinary event bus. A concealment request suppresses subsequent assistant,
tool, composer and notification output for that provider session. An untrusted
restore request cannot undo suppression. Restarting the provider session
releases that temporary suppression, but cannot grant an Owner Session.

The server has no verified private client channel across web, desktop, mobile,
remote subscriptions, durable snapshots and exports. It must not report
`concealed: true` until that channel exists. Existing ordinary structured
questions remain unsuitable for passwords. Successful T3 unlock and a full
memory round trip remain unavailable and are release blockers.

Direct Codex and other provider adapters do not receive recall packets. Their
ordinary saved turn inputs are not a disposable context. Pi's
`before_provider_request` hook is the only integration point for permitted
recall evidence; provider-owned history isolation still requires live
verification before rollout.
