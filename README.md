# Modular Recursive Plugin Pipeline

## Stitched Hypervisor Build

Modular Recursive Plugin Pipeline is a browser-based JavaScript workbench for
constructing, running, inspecting, and supervising ordered pipelines of reusable
processing modules. JSON is the primary working representation; the included
text-processing examples make the movement of data, state, and control visible.

Assemble stages, change their order, run a pass, and inspect what each stage did.
Named pipelines support branching and return to the caller. Document snapshots,
plugin state, lifecycle hooks, and a separate hypervisor view provide several
ways to examine an execution.

The build combines a runtime, plugin factories and registries, stateful and
stream-oriented stages, control-flow extensions, advanced data plugins, and
registered-callable reconstruction.

## Start with the browser

1. Download or clone this repository and keep the `source/` directory intact.
2. Open [source/index.html](source/index.html) in a browser.
3. Choose input in **Upload / Input**, then return to **Main Run**.
4. Use **Run Once** to inspect a single pass. Use the loop and signal controls
   when you want repeated execution, pause/resume, or stop/reset.
5. Inspect **Debug** and **Hypervisor** alongside the JSON output.

The browser application uses the included classic scripts. There is no package
installation or build step for this entry point.

| View | Purpose |
|---|---|
| Main Run | Execution controls, runtime status, and output. |
| Upload / Input | JSON text, object-mode input, or a selected JSON file. |
| Configuration | Plugin loading, removal, order, and branching controls. |
| Debug | Registry, state, diagnostics, errors, and execution inspection. |
| Hypervisor | Observed event counts, timings, alerts, and oversight snapshots. |

The file-input adapter limits selected JSON files to 2,000,000 bytes. Input and
output are handled by the browser adapters; an output snapshot is displayed as
formatted JSON.

## Follow the demonstration

The default main pipeline reads input, records inspection state, checks whether
the next stage should run, filters with a regex, tokenizes, visits a secondary
inspection pipeline, evaluates a break signal, records a pass-through revision,
counts words, and emits output.

```text
main
  start → reader → inspect → conditional skip → regex filter → tokenizer
        → secondary junction → break signal → pass-through → word count → output → stop

secondary
  start → inspect → stop → return to the caller
```

These defaults demonstrate the machinery. They are examples to inspect and
change, rather than a universal processing specification.

## Documents, state, and control

- **Document history:** a document retains an immutable original, a current JSON
  snapshot, transformation snapshots, and aligned history metadata.
- **Stateful stages:** readers, tokenizers, regex search, diagnostics, word
  counting, and output share an execution context.
- **Stream-oriented stages:** original and transformed documents are kept
  separately, with transformation history available in output.
- **Control flow:** named-pipeline junctions, conditional skipping, break
  signals, and inspection frames make execution decisions observable.
- **Advanced processing:** conditional and external-signal branches, per-item
  operations, child-plugin execution, and JSON/object conversion extend the
  built-in examples.
- **Registered callables:** trusted factories reconstruct behavior from a
  registered type and explicit constructor/internal state.

The runtime passes a shared context through its stages. That context carries
the current document, derived analysis, plugin state, output, diagnostics,
errors, and control-flow records.

## Observation and execution limits

Lifecycle hooks run outside the processing-stage sequence. The hook bus has
registration priorities and callback-failure policies. The hypervisor uses that
bus to collect event counts, timings, failures, skips, topology information,
alerts, and snapshots; it also provides runtime control methods.

The hypervisor operates inside the same JavaScript environment as the runtime.
It is an application oversight component, not an independent isolation boundary.

The source includes node and branch budgets, cooperative elapsed-time checks,
selected input/match caps, strict JSON cloning, and bounded retention for some
observation records. Cooperative checks do not interrupt arbitrary synchronous
work between check points. Treat the supplied safeguards as specific contracts
to test, rather than a general guarantee about every plugin.

## Source map

| File or directory | Responsibility |
|---|---|
| [pipeline-core.js](source/pipeline-core.js) | Documents, validation/cloning, generic state, hooks, factories, registries, plugin management, and the recursive runtime. |
| [stateful-plugins.js](source/stateful-plugins.js) | Stateful input, analysis, diagnostics, and output. |
| [stream-plugins.js](source/stream-plugins.js) | Original/transformed document processing and history-aware output. |
| [control-flow-plugins.js](source/control-flow-plugins.js) | Named pipelines, junctions, skipping, break signals, inspection, and runtime-method extensions. |
| [advanced-flow-data-plugins.js](source/advanced-flow-data-plugins.js) | Branch predicates, per-item processing, child plugins, and conversions. |
| [pipeline-hypervisor.js](source/pipeline-hypervisor.js) | Lifecycle observation, metrics, alerts, topology checks, and controls. |
| [browser-adapters.js](source/browser-adapters.js) and [application-ui.js](source/application-ui.js) | Browser input/output and the tabbed application. |
| [callable-registry/](source/callable-registry/) | ESM registered-callable module and its standalone self-test. |
| [browser callable variant](source/deep-clone-callable-registry.browser.js) | The browser/global entry point used by the assembled application. |
| [self-tests.js](source/self-tests.js) | Included runtime assertions. |

The application composes scripts in a defined order. In particular, the
control-flow extension modifies runtime prototype methods. Read the assembled
load order in [index.html](source/index.html) before treating the core file as
the complete application.

## Run the included checks

From the repository root, with Node.js available:

```sh
node source/run-self-tests-node.js
node source/callable-registry/self_test.mjs
```

The main harness contains 16 self-tests. Both entry points passed during the
clean-build review. That establishes those assertions; it does not establish
complete coverage of the advanced plugins or a successful browser session.

The supplied [historical result](source/SELF_TEST_RESULT.json) is retained.
The [import validation record](validation/initial-import-20261029T165409Z.json)
records a separate run, environment information, and source hashes with private
machine identifiers generalized. Future executions should be recorded against
the source actually tested.

## Work on the project

Start with a small pipeline and one processing family. Follow a factory through
registration, loaded-instance creation, execution, state updates, and output.
When changing control flow, also inspect the runtime extensions and hook events.
When changing cloning, compare the strict JSON boundary with the separate
registered-callable contract.

Useful review targets include history-update consistency, writable reference
ownership, nested-work accounting, observer completion records, configuration
identity, and error propagation. Add an assertion for the contract being changed
and keep the original input and diagnostic evidence inspectable.

## Version and license status

The browser title and original README identify v12. The technical notes document
changes through v12.4. This repository preserves that distinction; it does not
declare a newly reconciled release version.

See [source/README.md](source/README.md) for the existing development notes and
API examples. No standalone project license is included in this tree; existing
notices in supplied files remain unchanged.
