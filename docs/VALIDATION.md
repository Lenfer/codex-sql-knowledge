# Validation and limits

Version: `0.2.0.dev3`. This is a development release.

On 12 November 2026 the source project's suite passed 108 tests in 88.455 seconds
on macOS, Python 3.14.6 and SQLite 3.53.4. The environment used MCP 1.26.0 and
Pydantic 2.14.1. `dependencies-current.json` records the environment and
`constraints-tested.txt` pins its Python dependencies.

The prepared public package passed the same 108 tests in 80.695 seconds. It keeps
the runtime source unchanged and places two historical recovery fixtures under
`tests/fixtures/legacy_runtime/`; three tests use those local fixture paths.
The documented demo also completed through real stdio MCP, including its full
audit with `status=complete`, `ok=true` and no reported problems.

The suite covers record lifecycle, revision conflicts, idempotent retries, writer
coordination, storage recovery, extension validation, resumable audits and real
stdio MCP sessions. The source project's own memory audit also completed with no
reported problems. These results do not establish performance at arbitrary data
sizes or compatibility with every supported dependency version.

Tests require local Unix sockets and child processes. The macOS sandbox used in
the initial run denied `socket.bind`; the same suite passed outside that sandbox.
Use an environment that permits the coordinator's local IPC. The test runner saves
each run under `work/tests-*/` and creates isolated test projects.

New WAL projects require SQLite 3.51.3+ with FTS5. Python's version does not establish
the version of its linked SQLite library. The checked dependency constraints target
the recorded environment; other Python, OS and dependency combinations need their
own validation.

The coordinator supports local processes for one OS user on one machine. Network
filesystems, distributed writers and cross-user isolation are outside that model.
Physical power loss and storage failure were not simulated. Use your own backup
and recovery procedure before relying on the memory for important records.

The skill and its reference guides are currently in Russian. The CLI and MCP
runtime accept Unicode data. The package contains an optional domain-specific
migration profile; it does not automatically adapt arbitrary legacy databases.

Run from the checkout with the installed package:

```bash
.venv/bin/python scripts/run_tests.py
.venv/bin/python examples/run_demo.py --project /absolute/path/to/new-demo
```


## Automatic audit lifecycle

The final 0.2.0.dev3 source suite passed 124 tests in 58.527 seconds on
17 November 2026. It covers real MCP startup/completion maintenance,
exact 20-error/one-success
retention, both core schemas, more than 1000 findings without truncation, failed
report saves, interrupted unlink recovery, late keep/fingerprint checks, open
files, symlinks, foreign identities, incomplete jobs and a model-free periodic
loop, parallel maintenance, marker-only historical directories and projects with
more than eight incomplete audits. Audit-start quotas apply only when explicitly
configured. Historical results above refer to the earlier 0.2.0.dev2 publication.

The final installed wheel passed 16 focused regression tests in 20.511 seconds,
including real MCP sessions and the removal of implicit admission quotas. The
preceding installed build passed the full 123-test suite in 162.677 seconds;
the subsequent correction changed the admission defaults and added its regression.
The final wheel's Python modules matched the source byte for byte. Its SHA-256 is
`01e8047f752e7a034051e0c695199b064310bff7f68b52ef095e6c131a5c3501`.
A full audit of the source project completed without problems; its report remained
readable and its snapshot directory was automatically released.
