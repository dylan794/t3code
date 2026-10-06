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

## Private unlock channel

The dedicated `owner-private.subscribe` stream carries content-free conceal,
secret, cancellation and restore frames. `owner-private.respond` returns the
secret directly to the provider transport. These requests do not enter the
orchestration command/event pipeline or ordinary RPC tracing. The client
unmounts its ordinary workspace and clears thread, shell and composer caches
before acknowledging concealment. Every connected client must acknowledge;
an old client without this channel prevents unlock.

The initiating WebSocket connection owns the private request. Other clients
remain concealed. Secrets expire after sixty seconds and are cancelled on
disconnect, lock and provider stop. A Core password check against the exact
enrolled host and actual Pi model endpoint must succeed before the password is
forwarded to Pi. A separate transport-only Core grant checks every presentation
and is never renewed merely because the host retains it. Restore requires a
fresh Core check. Model changes revoke it. A one-second Core monitor detects
silent lock or service loss; output dispatch checks do not wait for that poll.
The acknowledgement fence is checked again when accepting a secret and when
presenting content, so a newly connected client cannot reuse an earlier
concealment check. Grant expiry sends concealment instead of leaving restored
content visible. A late authentication or presentation reply cannot replace or
revoke a newer grant.

Ordinary provider claims have no private epoch and do not conceal the workspace
when their connection ends or another client subscribes. Private Pi output stays
fenced after its owning client disconnects. Interrupting a private turn cancels
its pending secret before sending the provider abort. Private stream teardown
conceals restored client content on transport loss, before reconnect; a new
server session resets only the epoch comparison and retains the content lock.

HTTP snapshots, thread search, websocket snapshots, replay and live thread
events redact locked Pi thread content. Private restored content stays volatile
in clients and is not written to their ordinary caches. Ordinary structured
questions remain unsuitable for passwords. Focused transport tests prove a
successful unlock through a synthetic Core callback. Actual client/model and
Windows/Mac route verification remain release gates, including the interval
between silent Core revocation and client concealment.

Direct Codex and other provider adapters do not receive recall packets. Their
ordinary saved turn inputs are not a disposable context. Pi's
`before_provider_request` hook is the only integration point for permitted
recall evidence; provider-owned history isolation still requires live
verification before rollout.
