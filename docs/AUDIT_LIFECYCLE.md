# Audit lifecycle, runtime 0.2.0.dev3

An audit checks a stable SQLite backup at a specific revision. It never repairs
the live database. Before fixing a finding, read the current records, recheck the
violation, and use revision-checked writes. A successful old audit does not certify
later mutations.

## Automatic maintenance

Ordinary runtime code saves the result of a `complete` audit and releases its
snapshot and scratch indexes. No language model runs during maintenance. It runs
after completion, at stdio MCP startup and every 300 seconds while that MCP lives.
The MCP lifespan owns the timer; no OS daemon, cron job, Codex scheduled task or
Disk Budget hook is installed. Multiple MCP processes submit maintenance through
the existing single project coordinator and its transaction lease.

Reports use `.tmp/check-reports/reports.sqlite`, a disposable service cache bound
to the project ID, canonical root and database generation. It is separate from
`knowledge.sqlite`: deleting an old report also deletes its problem rows, without
leaving copies in append-only knowledge events. SQLite auto-vacuum reclaims pages
inside this report store. Core project schemas and recorded knowledge are unchanged.

Keep the most recent 20 reports with problems and one successful report, ordered
by completion timestamp and job ID for ties. Each result identifies its date,
snapshot revision and outcome; technical job/snapshot identifiers allow existing
clients to retrieve it. Error reports additionally retain every reason and affected
record/table/event identifier, without truncating the number of findings.
`project_check_reports` lists summaries and the latest maintenance outcome.
`project_check_report(job_id, offset, limit)` still pages problems after the snapshot
has gone. An expired result returns `audit_report_unavailable`; report expiry does
not invalidate current project knowledge. Explicitly save evidence you need longer
as a separate project artifact or knowledge record.

## Save before delete

1. Lock the job against cooperating audit steps and physical workers. Validate its
   project, generation and `complete` status. Running, failed and cancelled jobs
   never qualify for automatic deletion.
2. Refuse symlinks, unknown files, live handles and catalog-owned/keep data.
   `lsof` must positively establish inactivity; unavailable or uncertain evidence
   leaves the files intact.
3. Save the report and the exact fingerprinted unlink intent in one durable
   transaction in the service cache. Re-read and verify the report before unlink.
4. Revalidate the file set and catalog protection, then unlink only the captured
   regular files. Remove state last and empty captured directories with `rmdir`.
   There is no recursive tree deletion or arbitrary SQLite cleanup.
5. Clear the cleanup intent after release. Prune retained reports to 20 errors
   and one success. A report needed by an interrupted cleanup stays protected
   until that operation finishes; this pending-operation exception can temporarily
   exceed the normal retained-report count.

A failed save preserves all snapshot files. A failed unlink preserves the report
and exact intent so a later sweep can resume. A changed fingerprint, a late keep
registration or a live consumer blocks the retry. `cache_state=pending` and the
maintenance result expose a deferred release; the audit's own result remains
separate. CLI `check-maintain` or MCP `project_check_maintenance` retries maintenance
without waiting for the next timer. It does not cancel or advance incomplete jobs.

Each sweep attempts at most 16 completed jobs. Older completed jobs from the same
project are eligible for the same save-first process. Incomplete jobs, old manual
cleanup receipts and user knowledge are not purged by this feature.

Audit-start quotas are opt-in: `[audit_cache] max_jobs` and/or `max_bytes`
positive integer settings. There is no implicit eight-job/8 GiB limit on existing
projects. Explicit byte accounting allows twice the main database size as start
headroom; it is not a filesystem quota or a Disk Budget reserve. A quota never
deletes incomplete jobs or blocks their continuation. Historical marker-only
folders remain untouched and count toward byte accounting, not live-job slots.
Bounded striped locks avoid one permanent lock file per historical audit.

## Installation and scope

The release requires no migration or recreation of project knowledge. Existing
MCP processes and writer coordinators must restart to execute the new Python
code. A `current` symlink switch alone cannot replace code already loaded in a
running process. No shared writer is force-stopped by the installer.

MAVIS model, artifact and experiment cleanup remains manual under its own policy.
The automatic exception is SQL Knowledge's own completed audit cache only. Existing
protected catalog registrations stay protected; this feature does not reclassify
them or erase historical cleanup evidence.
