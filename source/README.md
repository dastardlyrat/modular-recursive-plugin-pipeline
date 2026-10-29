# Merged Modular Recursive Plugin Pipeline v12

Open `index.html` directly in a browser.

## v12 addition: out-of-band hook bus

This build adds a robust pipeline execution hook bus on top of the v11 JSON factory, stream pipeline, and control-flow modules.

The hook bus observes execution without being inserted as a pipeline module. It records and dispatches lifecycle events for:

```text
pipeline-execution-started
pipeline-execution-finished
pipeline-execution-failed
module-execution-started
module-execution-finished
module-execution-failed
module-execution-skipped
junction-pipeline-entered
junction-pipeline-returned
filter-decision-recorded
break-signal-recorded
inspection-snapshot-recorded
context-patch-applied
```

### Hook bus security and robustness

- Hook registration records are validated before use.
- Duplicate hook registration IDs are rejected.
- Hook event payloads are cloned and deeply frozen before dispatch.
- Hooks receive compact context summaries rather than the full mutable pipeline context.
- Hook failures are recorded and do not stop the pipeline by default.
- Hooks can be disabled after failure or configured to fail the pipeline explicitly.
- Hook dispatch records and hook failure records are visible in the debug pane.

## v12.1 addition: JSON document history buffer

`PipelineJsonDocument` now keeps an append-only JSON history buffer in addition to its latest active JSON snapshot.

### History model

```text
immutableOriginalRoot
  Frozen clone of the first accepted JSON input. This is never rewritten.

currentJsonRoot
  Frozen latest active JSON snapshot. Existing plugins continue to read this as the current document.

jsonHistoryBuffer
  Frozen array of frozen JSON snapshots. Each document transformation appends a new snapshot.

jsonHistoryMetadata
  Frozen array of metadata records aligned with `jsonHistoryBuffer` by history index.
```

### New document methods

```text
appendTransformationSnapshot(nextJsonCompatibleValue, transformationMetadata)
toHistoryPlainArray()
toHistoryMetadataPlainArray()
getJsonHistoryLength()
getLatestHistoryIndex()
```

### Output visibility

- Stateful output now includes `dataHistory` and `dataHistoryMetadata`.
- Stream output now exposes transformed document history under `streamState.transformedJsonHistory` and `streamState.transformedJsonHistoryMetadata`.
- Stream transformations append to the transformed document history instead of replacing the transformed document wrapper each time.


### Core hook bus classes

```text
PipelineExecutionHookBus
PipelineExecutionHookRegistrationRecord
PipelineExecutionHookEventFactory
PipelineExecutionHookSecurityGuard
```

### Hook failure policies

```text
record-and-continue
disable-hook-and-continue
fail-pipeline-execution
```

## v11 control-flow modules retained

- `plugin.control.junction.toNamedPipeline`
  - Enters another named pipeline from that pipeline's own `system.start` sentinel.
  - Default target: `secondary`.
  - Returns to the calling pipeline after the secondary pipeline reaches its `system.stop` sentinel.

- `plugin.control.filter.nextModuleBySignal`
  - Reads the runtime external signal named `allow-next-module`.
  - If the signal is `true`, execution continues normally.
  - If the signal is `false`, the runtime skips exactly the next non-system module and then continues after it.

- `plugin.control.break.signal`
  - Reads the runtime external signal named `break-control-signal`.
  - Accepts `NONE`, `STOP`, `PAUSE`, and `RESUME`.
  - `STOP` stops execution.
  - `PAUSE` pauses execution after the break module.
  - `RESUME` resumes execution.
  - `NONE` leaves execution unchanged.

- `plugin.control.inspect.pipeline`
  - Captures the execution frame at that pipeline position.
  - Records the current pipeline name, node index, node order, loaded named pipelines, current JSON data, diagnostic count, and error count.

## Named pipelines

The runtime initializes two pipeline managers:

```text
main
secondary
```

The browser default loads the `main` pipeline with stream processing plus control-flow modules. It loads the `secondary` pipeline with an inspection module so the junction can enter the secondary pipeline from its start sentinel.

## Default main stream pipeline

```text
system.start
plugin.stream.reader
plugin.control.inspect.pipeline
plugin.control.filter.nextModuleBySignal
plugin.stream.regex.filter
plugin.stream.tokenizer
plugin.control.junction.toNamedPipeline
plugin.control.break.signal
plugin.stream.dummy.passThrough
plugin.stream.wordCount
plugin.stream.output
system.stop
```

The conditional filter is placed immediately before `plugin.stream.regex.filter`, so setting `allow-next-module` to `false` skips the regex filter only. The tokenizer, junction, break module, word count, and output continue after that skip.

## Default secondary pipeline

```text
system.start
plugin.control.inspect.pipeline
system.stop
```

## Built-in self-tests

Run from Node:

```bash
node run-self-tests-node.js
```

Current included tests verify:

- stream transformation still preserves original JSON and counts transformed words
- unload removes the selected plugin instance
- conditional skip continues execution after skipping the next module
- junction enters the secondary pipeline and captures a secondary inspection record
- break signal can pause execution before the following module
- hook bus lifecycle hooks fire in deterministic order
- hook event payloads are frozen and context summaries are redacted
- hook failures are recorded and can continue without stopping the pipeline
- JSON document history buffers preserve immutable frozen snapshots

## v12.2 generic plugin state handler

This build adds a generic plugin state layer so output snapshots do not need to hardwire plugin-specific fields such as `tokenizerState` and `regexState`.

### Added context state shape

```text
pipelineContext.state
  byInstanceId
  byTypeId
  timeline
```

### Added `PipelineContextUtility` helpers

- `ensureGenericStateContainer(pipelineContext)`
- `setPluginState(pipelineContext, pluginStateOwner, stateNamespace, stateValue, stateOptions)`
- `getPluginStateByInstanceId(pipelineContext, pluginInstanceId, stateNamespace, defaultValue)`
- `getLatestPluginStateByTypeId(pipelineContext, pluginTypeId, stateNamespace, defaultValue)`
- `createGenericStateSnapshot(pipelineContext, stateSnapshotPolicy)`

### Updated behavior

- `createFreshPipelineContext()` now initializes `state`.
- `createStatefulOutputSnapshot()` now emits a generic `state` snapshot.
- Stateful tokenizer, regex search, and word-count plugins write through the generic state handler.
- Stream tokenizer, regex filter, and word-count plugins also mirror their state into the generic handler.
- `tokenizerState` and `regexState` remain as temporary compatibility aliases until tests, UI output, and downstream consumers are migrated.

### Added self-test

- `generic state handler stores stateful tokenizer regex and word-count records`

## v12.3 stitched orchestration build

This build stitches together the uploaded parts:

```text
merged_modular_pipeline_v12_2_generic_state_handler.zip
  Core runtime, hook bus, named pipelines, stateful plugins, stream plugins, control-flow plugins, self-tests.

tabbed_ui_only_files.zip
  Tabbed browser UI with Main Run, Upload/Input, Configuration, Debug, and Hypervisor tabs.

advanced_flow_data_plugins_only.zip
  Advanced branching, external-signal branching, for-each JSON function execution, for-each plugin runner, and JSON/object emitter plugins.

deep_clone_callable_registry_hardened(1).zip
  Registered callable deep-clone module retained as an ESM module and exposed through a browser/global shim.
```

### New hypervisor layer

`pipeline-hypervisor.js` adds an abstract oversight layer above the runtime. It is not inserted as a pipeline plugin. Instead, it attaches to the existing hook bus and observes lifecycle events out-of-band.

The hypervisor records:

- runtime health state
- observed hook event counts
- pipeline timing metrics by named pipeline
- module timing metrics by plugin type
- module failure counts by plugin type
- module skip counts by plugin type
- control-flow activity by source type
- namespace availability for core, stateful, stream, control-flow, advanced, callable-registry, and hypervisor modules
- recent alerts, diagnostics, failures, and event records
- named-pipeline topology snapshots
- hook-bus registration and dispatch snapshots

### Hypervisor tab

Open `index.html`, then select the `Hypervisor` tab. The tab shows compact values for:

```text
Health
Observed Events
Alerts
Avg Main Pass ms
```

It also renders the full hypervisor snapshot as JSON.

### Hypervisor API

```js
const hypervisor = PipelineHypervisor.createDefaultPipelineHypervisor({
  runtime,
  debug,
  slowPipelineThresholdMs: 250,
  slowModuleThresholdMs: 75
});

const snapshot = hypervisor.createSnapshot();
await hypervisor.runOnce();
hypervisor.executeSignal(PipelineCore.Signal.STOP);
```

### Callable registry integration

Two callable-registry entry points are included:

```text
callable-registry/deep_clone_callable_registry_module_hardened.js
  Original ESM module.

deep-clone-callable-registry.browser.js
  Browser/global shim exposed as PipelineCallableRegistry.
```

The browser/global shim is loaded before `pipeline-hypervisor.js` and before `self-tests.js`, so both the UI and the test harness can verify registered callable cloning.

### Updated self-tests

`node run-self-tests-node.js` now loads:

```text
pipeline-core.js
stateful-plugins.js
stream-plugins.js
control-flow-plugins.js
advanced-flow-data-plugins.js
deep-clone-callable-registry.browser.js
pipeline-hypervisor.js
self-tests.js
```

The current self-test output is saved in `SELF_TEST_RESULT.json` and reports `passed: true` for the stitched build.

## v12.4 hardening pass

This build hardens the stitched orchestration layer in the order selected during review.

### 1. Hypervisor junction oversight fix

The hypervisor now listens to the actual hook phase names exported by the runtime:

```text
junction-pipeline-entered
junction-pipeline-returned
```

The previous oversight checks referenced non-exported shorthand names, which made the hypervisor partially blind to junction enter/return activity. A self-test now proves that both junction phases are observed.

### 2. Runtime execution budgets

The runtime now carries explicit pass-level budget controls:

```text
maximumNodesPerPass
maximumElapsedMsPerPass
maximumBranchEntriesPerPass
```

Each pass resets a budget snapshot and records node execution and branch-entry consumption. When a budget is exceeded, execution fails closed with diagnostic metadata in the runtime error record.

The runtime also prevents overlapping `runOnce()` executions through `isPipelinePassRunning`. The browser UI mirrors this with an async action lock so repeated clicks cannot start concurrent passes.

### 3. Clone semantics split

`PipelineSecurityUtility.deepCloneJsonSafeValue()` remains as a compatibility alias, but strict JSON boundaries now use:

```js
PipelineSecurityUtility.cloneStrictJsonValue(value)
```

Strict JSON cloning accepts only JSON-compatible scalars, dense arrays, and plain enumerable data objects. It rejects runtime objects and descriptor tricks, including:

```text
Map
Set
Date
RegExp
ArrayBuffer / typed arrays
functions
symbols
bigint
undefined
accessor properties
non-enumerable own properties
symbol keys
__proto__ / prototype / constructor object keys
circular references
non-finite numbers
```

This is stricter than ordinary `JSON.stringify()` behavior. It intentionally rejects ambiguous or prototype-polluting object shapes instead of silently rewriting them.

### 4. Regex hardening

Regex creation now includes a conservative ReDoS guard before constructing the `RegExp`. The guard rejects obvious catastrophic-backtracking structures such as nested quantified groups and repeated wildcard quantifiers.

Regex scanning is also bounded by:

```text
maximumMatchCount
maximumInputLength
```

Current default input cap for regex plugins is `100000` characters.

### 5. Expanded negative self-tests

The self-test harness now covers the hardening behavior directly:

- hypervisor observes junction enter and return hook phases
- strict JSON clone rejects runtime objects and descriptor tricks
- regex ReDoS guard rejects nested quantified groups
- execution node budget aborts excessive passes
- execution branch budget aborts excessive junction traversal
- callable registry shim still supports registered callable deep cloning

Current verification command:

```bash
node run-self-tests-node.js
```

Expected result:

```json
{
  "passed": true
}
```
