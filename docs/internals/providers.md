# Provider architecture

> For maintainers. Using T3 Code? See [docs/user](../user/).

A provider is the agent runtime that does the actual work. T3 Code supports several, and the
orchestration layer does not know which one is behind a thread.

## Built-in drivers

[`builtInDrivers.ts`][drivers] exports `BUILT_IN_DRIVERS` with the stock drivers plus the
fork-only Jarvis provider:

| Driver kind   | Driver source                           |
| ------------- | --------------------------------------- |
| `codex`       | [`Drivers/CodexDriver.ts`][codex]       |
| `claudeAgent` | [`Drivers/ClaudeDriver.ts`][claude]     |
| `cursor`      | [`Drivers/CursorDriver.ts`][cursor]     |
| `grok`        | [`Drivers/GrokDriver.ts`][grok]         |
| `opencode`    | [`Drivers/OpenCodeDriver.ts`][opencode] |
| `pi`          | [`Drivers/PiDriver.ts`][pi]             |

The `pi` driver launches Pi in RPC mode with only the configured Jarvis extension. T3 Code replaces
Pi's terminal presentation while Pi remains the coordinating runtime and Jarvis keeps ownership of
its job manager and action policy. One scoped Pi process is owned by each active T3 thread.

### Pi interactions

The Pi adapter translates extension UI requests into T3's provider-neutral interaction events:

- Pi `confirm` dialogs become approval cards. Approve, decline, and cancel are supported. Pi does
  not provide session-persistent confirmation semantics, so these cards do not offer "always allow
  this session."
- Pi `select`, `input`, and `editor` dialogs become structured user-input cards. They can be
  cancelled without interrupting the turn. Select values round-trip exactly, including surrounding
  whitespace, while text and editor dialogs can submit empty values and preserve editor defaults.
- Pi timeouts, turn interruption, session stop, and process exit resolve the corresponding T3 card
  so stale interactions are not left in the thread.

Structured answers are persisted with the thread like other provider events. They are for ordinary,
non-secret input only. Pi's RPC dialog protocol does not identify masked or secret fields, so API
keys, passwords, and tokens need a separate ephemeral input path before they can be collected in
this interface.

### Pi metadata and rollback

T3 title, branch, commit, and change-request generation uses a separate ephemeral Pi JSON process.
That process receives the selected Pi model but loads no session, extension, skill, prompt template,
context file, or tool. T3 validates the final assistant message against the same output schema used
by the stock providers, then applies the existing title and Git text sanitizers.

Pi rollback uses Pi's native session tree instead of trimming only T3's in-memory transcript. The
adapter reads `get_fork_messages`, forks immediately before the oldest turn being removed, refreshes
Pi's session state, and returns the replacement resume cursor. `ProviderService` persists that
cursor, so a stopped or failed process resumes from the rolled-back session rather than the old
branch. Rollback rejects an active turn and accepts requests larger than the current history as a
full rollback.

Metadata child processes have a three-minute deadline and are killed when they time out. Invalid
JSON, invalid schema output, provider errors, missing runtime files, and nonzero exits return typed
text-generation errors. The live Pi integration test uses a disposable Pi profile, Jarvis
configuration root, and local model endpoint to verify metadata generation, native rollback,
process restart, session resume, and a successful follow-up turn without reading or writing T3's
installed application data or the user's Jarvis authentication state.

Each driver declares its `driverKind`, a `configSchema`, and a `create` function that builds an
adapter in a child scope. Adapter implementations live beside them in
`apps/server/src/provider/Layers/` (`CodexAdapter.ts`, `ClaudeAdapter.ts`, and so on) and conform to
[`ProviderAdapter.ts`][adapter]. Read the driver plus its adapter to see how a specific agent's
transport, config, and event shapes are mapped.

## Registry and routing

Two registries separate configuration from live processes:

- [`ProviderInstanceRegistry`][instances] keys configured instances by `ProviderInstanceId`. Creating
  one looks up the driver by `driverKind`, decodes `entry.config` with that driver's schema, opens a
  child scope, and calls `driver.create`.
- [`ProviderAdapterRegistry`][registry] resolves an instance ID to its live adapter via
  `getByInstance`.

[`ProviderService`][service] sits on top. It combines the adapter registry with the provider session
directory to route session and turn operations for a thread, so callers name a thread, not an agent.

Adding a driver means writing the driver plus adapter and adding it to `BUILT_IN_DRIVERS`. No
orchestration, contract, or client change is required for the common case.

## How provider work is requested

Clients never call a provider directly. They dispatch orchestration commands over the RPC method
`orchestration.dispatchCommand`, defined with the rest of the orchestration surface in
[`orchestration.ts`][contracts]. The client-dispatchable provider-facing commands are
`thread.turn.start`, `thread.turn.interrupt`, `thread.approval.respond`,
`thread.user-input.respond`, `thread.checkpoint.revert`, and `thread.session.stop`, plus the mode
setters `thread.runtime-mode.set` and `thread.interaction-mode.set`.

The engine persists an event for the command, and a server-side reactor performs the provider call.
Provider output comes back as internal commands such as `thread.message.assistant.delta` and
`thread.session.set`, which clients observe through `orchestration.subscribeThread`. See
[overview.md](./overview.md) for the command/event loop.

## Server-side workers

Provider work flows through three queue-backed workers. All three are built with
`makeDrainableWorker` from [`DrainableWorker.ts`][worker] and expose `drain` for deterministic test
synchronization.

1. [`ProviderRuntimeIngestion`][ingest] consumes provider runtime streams and emits orchestration
   commands.
2. [`ProviderCommandReactor`][cmd] reacts to orchestration intent events and dispatches provider
   calls.
3. [`CheckpointReactor`][checkpoint] captures workspace checkpoints on turn start and completion, and
   performs reverts.

### Buffered assistant delivery

A thread in `buffered` assistant delivery mode accumulates assistant text instead of streaming each
delta. The buffer is not held until turn completion. In [`ProviderRuntimeIngestion`][ingest],
`MAX_BUFFERED_ASSISTANT_CHARS` is 24,000: the append that would exceed it invalidates the buffer and
spills the whole accumulated text as one delta. The buffer also flushes at interaction boundaries,
when a request opens (approval) or user input is requested, via
`flushBufferedAssistantMessagesForTurn`.

[drivers]: ../../apps/server/src/provider/builtInDrivers.ts
[codex]: ../../apps/server/src/provider/Drivers/CodexDriver.ts
[claude]: ../../apps/server/src/provider/Drivers/ClaudeDriver.ts
[cursor]: ../../apps/server/src/provider/Drivers/CursorDriver.ts
[grok]: ../../apps/server/src/provider/Drivers/GrokDriver.ts
[opencode]: ../../apps/server/src/provider/Drivers/OpenCodeDriver.ts
[pi]: ../../apps/server/src/provider/Drivers/PiDriver.ts
[adapter]: ../../apps/server/src/provider/Services/ProviderAdapter.ts
[instances]: ../../apps/server/src/provider/Services/ProviderInstanceRegistry.ts
[registry]: ../../apps/server/src/provider/Services/ProviderAdapterRegistry.ts
[service]: ../../apps/server/src/provider/Layers/ProviderService.ts
[contracts]: ../../packages/contracts/src/orchestration.ts
[worker]: ../../packages/shared/src/DrainableWorker.ts
[ingest]: ../../apps/server/src/orchestration/Layers/ProviderRuntimeIngestion.ts
[cmd]: ../../apps/server/src/orchestration/Layers/ProviderCommandReactor.ts
[checkpoint]: ../../apps/server/src/orchestration/Layers/CheckpointReactor.ts
