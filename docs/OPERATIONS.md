# Operating a project

Pass `--project /absolute/root` to each CLI or MCP invocation. Keep the generated
`knowledge.toml` with its database, including the instance identity. Opening an
existing database does not silently migrate it or switch its journal mode.

## Writes and retries

Write envelopes identify the actor, reason, durable source and idempotency key.
Active findings need evidence; accepted decisions also need an authorization
reference. Read `catalog_get.record.revision` before changing an entity and send
`expected_revision`. For relationships, send the revisions of affected entities
in `expected_revisions`.

After a revision conflict, read the current records and form a new request with a
new key. After `outcome_unknown`, repeat the original envelope with its original
key to discover whether it committed. A timeout alone does not prove rollback.

The writer uses a private Unix socket for processes belonging to the same local
user. This is a local IPC boundary, not a network authorization service. The queue
is bounded and held in RAM. A success receipt follows the database commit.

## Maintenance

1. Stop the project's MCP clients.
2. Run `writer-stop` and wait for the writer's lock and socket to be released.
3. Save a consistent authorized backup before changing files.
4. Apply the intended migration or restore and check identity and integrity.
5. Run `writer-start`, then restart MCP clients with the explicit project root.

`writer-stop` leaves a maintenance fence. New writes cannot restart the writer
until an explicit `writer-start`. An idle exit is different: the next write can
start the coordinator again.

After changing `knowledge.toml` or an extension manifest, restart clients. Clients
bound to an earlier contract must not continue writing. Extension migrations
support declared schema changes; use typed operations for row data.

## Full audit

`project_check_start` creates a snapshot. Call `project_check_step(job_id, batch_size)`
until a terminal status, then `project_check_report(job_id, offset, limit)` to read
the results. A running, failed or cancelled job is not a pass. A job pins project
identity and snapshot metadata. CLI `check` waits for all phases.

Preserve audit results you need before explicitly cleaning snapshots. The runtime
does not delete project evidence when a file goes missing; reconciliation keeps
the record and history.

## Extensions and compatibility

Define project extensions in versioned manifests. Keep their schemas and migrations
with the project. Use attributes when a separate table adds no useful constraints.
The bundled demo shows a small extension; the optional `mavis` profile handles a
specific existing building/scene model.

Legacy writers need an explicitly enabled compatibility adapter. Installing a new
runtime does not authorize migration of existing project memory. Test migration
on a selected copy and verify record counts, provenance and rollback before any
live switch. The runtime does not provide a generic arbitrary SQL write tool.
