# DK-812 — Deletion-Preserving Load Mode

## Approaches considered

Several designs satisfy “retain absent rows, stamp when removed, restore on reappearance.” The main forks are **how the marker is represented**, **who owns the schema**, and **how writes are executed**.

| Approach | Idea | Pros | Cons |
|----------|------|------|------|
| **A. Extend `full_compare` with a flag** | Same MERGE as today, but replace `whenNotMatchedBySourceDelete` with an update when `preserve_deletions: true`. | One mode to learn; minimal enum surface. | Mixes physical-delete and soft-delete semantics in one loader; harder to grep in Terraform/ops metadata; validation rules diverge inside one class. |
| **B. Domain-declared marker in YAML** | Model authors add e.g. `is_deleted` / `deleted_at` to `columns` and the engine only sets those fields. | Flexible naming; visible in contracts and BI catalogs. | Every audit table repeats the same column pair; Terraform must stay in sync; risk of wrong types/nullability; teams can omit or misuse the columns. |
| **C. Boolean active flag only** | `is_active = false` when absent from source (engine- or domain-owned). | Simple filters for “current” rows. | Does not meet the ticket’s **when** requirement without a second column; booleans are weaker for audit narratives. |
| **D. SCD Type 2 / history table** | Close current row, insert new version on change; deletes become end-dated versions. | Rich temporal analytics. | Heavy schema and query patterns; overkill for “keep deleted rows + one timestamp”; large diff from existing `full` / `full_compare` loaders. |
| **E. Append-only delete log** | Main table stays as `full_compare`; separate `*_deletes` table records removals. | Clear separation of current vs audit. | Two tables to deploy and join; YAML and Terraform surface doubles; operators must discover both resources. |
| **F. Single MERGE, all branches** | One `merge()` with matched update, insert, and `whenNotMatchedBySourceUpdate` in a single pass. | One table scan in theory. | Harder to test and read; Delta’s string-based `set` maps discourage mixing “clear marker” and “set timestamp” in one expression block. |
| **G. Chosen: dedicated `soft_delete` mode + engine `_deleted_at`** | New `LoadMode`, nullable UTC timestamp column injected by the loader; two focused MERGE passes. | Matches existing mode pattern; audit timestamp built-in; idempotent “first seen absent”; domain YAML stays minimal. | Fixed column name; two MERGE operations; load-time clock (see Risk). |

## Design (chosen: G)

- **YAML interface:** domain teams set `refresh.mode: soft_delete` on the model (same pattern as `full` / `full_compare`). They declare **primary keys** on merge key columns with `primary_key: true`, exactly like `full_compare`.
- **Deletion marker:** the engine injects a single nullable **`TIMESTAMP`** column **`_deleted_at`** on the Delta target. It is **not** declared in model YAML — the loader adds it via `ALTER TABLE` when missing so existing tables can migrate without a Terraform column list change for that field.
  - **`NULL`** → row is **active** (present in source, or restored after reappearance).
  - **Non-null** → row is **inactive**; the value is the UTC instant when the row was **first** seen absent from the source (not updated on later runs — idempotency).
- **Runtime behaviour:** two Delta `MERGE` passes:
  1. **Upsert from source** (match on PKs): update all business columns from source, force `_deleted_at = NULL` (covers inserts, updates, and **reappearance** after soft delete).
  2. **Anti-join keys only:** rows in target with no matching source key and `_deleted_at IS NULL` get `_deleted_at` set to “now” once (`whenNotMatchedBySourceUpdate` with that condition).

### Why this approach

1. **Dedicated mode over a flag on `full_compare`** — Operators and Terraform already key off `refresh.mode`; a distinct value makes audit tables obvious in deployment metadata without reading YAML. The loader stays parallel to `FullCompareLoader` (same PK contract, opposite delete semantics) instead of branching physical vs soft delete in one class.

2. **Engine-injected `_deleted_at` over domain-declared columns (B)** — The ticket asks for a consistent invariant across teams; a single platform column avoids copy-paste schema mistakes and keeps Terraform column lists limited to business fields. `ALTER TABLE ADD COLUMN` when missing allows adopting the mode on existing Delta tables without redeploying every column definition.

3. **Timestamp over boolean-only (C)** — Auditors need *when* a record left the source; one nullable `TIMESTAMP` encodes both state (`NULL` = active) and event time (non-null = first absence). Reappearance is a single, clear rule: force `_deleted_at = NULL` on match.

4. **Two MERGE passes over one combined MERGE (F) or SCD/history (D, E)** — Pass 1 mirrors `full_compare` upsert behaviour with an explicit “clear marker on match.” Pass 2 only ships primary keys, minimizing shuffle for the anti-join. The conditional `whenNotMatchedBySourceUpdate` (`_deleted_at IS NULL`) gives **idempotency**: re-running with the same absent keys does not advance the timestamp. A history-table design would be the next step if domains need full row versions, not just deletion time.

5. **Validation at config time** — `soft_delete` without `primary_key: true` on any column fails like `full_compare`, so domain engineers get a pointed error before a cluster run.

## Trade-offs

- **Engine-owned column name** (`_deleted_at`) avoids YAML/schema drift (rejects approach B unless we add optional rename config later); downside is a fixed name if product needs per-domain conventions.
- **Two MERGE passes** (rejects F for readability) — simpler to reason about and test; small extra scan cost vs one merged statement.
- **In-place soft delete** (rejects D/E) — target row count grows with historical deletes but stays one table for BI; revisit SCD or a delete log if governance needs full before/after payloads.
- **Timestamp via SQL literal** in the merge `set` map — avoids passing `Column` objects into Delta’s string-based merge API; microsecond formatting is tied to Spark’s cast rules.

## Performance at large scale

The current loader is correct-first: two full `MERGE` operations over the target, pass 1 updating every matched row (no change detection). On billion-row tables that becomes shuffle-, file-, and commit-heavy. Likely improvements, in rough priority:

**MERGE execution**

- **Conditional matched updates (pass 1)** — Use `whenMatchedUpdate` / `whenMatchedUpdateAll` only when business columns differ (hash or column-wise `IS DISTINCT FROM`), so unchanged active rows are not rewritten. Cuts write amplification on wide tables where most keys are stable run-to-run.
- **Single MERGE (revisit F)** — Combine upsert and `whenNotMatchedBySourceUpdate` into one statement once semantics are proven, to avoid a second full target read and commit. Trade clarity and test surface for one scan where pass 2 dominates cost.
- **Skew handling** — Repartition (or salting) source and target keys on PK before merge; enable Delta merge skew hints / AQE on Databricks for hot keys.
- **Right-size the source** — If the upstream SQL already delivers CDC (only changed keys), merge on that subset instead of a daily full snapshot; pass 2 still needs “keys in target but not in source,” but pass 1 shrinks dramatically.

**Pass 2 (soft-delete stamp)**

- **Pre-filter target to active rows** — `whenNotMatchedBySourceUpdate` already requires `target._deleted_at IS NULL`; at scale, maintain **liquid clustering** or **partitioning** on `_deleted_at` (e.g. `active` vs `deleted` bucket) so the anti-join does not scan tombstoned history on every run.
- **Absent-key set explicitly** — For bounded batches, compute `target_keys LEFT ANTI JOIN source_keys`, persist that small DataFrame, and merge only those keys (or `UPDATE` via Delta `replaceWhere` on a PK list) instead of a global `whenNotMatchedBySourceUpdate` over the full table.
- **Skip pass 2 when provably empty** — If the pipeline guarantees no deletes (monotonic key space) or source row count equals target active count with a cheap check, short-circuit; document assumptions in config.

**Table layout and maintenance**

- **Cluster on PK** — Z-order / liquid cluster on primary-key columns so merge joins and file pruning hit fewer files.
- **Partition on domain keys** — If models are partitioned (date, region), ensure the source DataFrame carries partition predicates so Delta stats prune target files before merge.
- **Archive cold soft deletes** — Periodically move rows with old `_deleted_at` to an archive table or storage tier; keeps the hot merge set near “active + recently deleted” size.
- **OPTIMIZE / auto-compact** — Schedule compaction after large merges; soft-delete tables grow monotonically and fragment faster than `full_compare`.

**Operational**

- **Broadcast when source ≪ target** — Tiny daily deltas against a huge dimension table can use broadcast hash join hints on pass 1 (pass 2 remains target-heavy unless absent keys are materialized).
- **Idempotent retries** — Avoid re-running pass 1 with a new cluster timestamp if pass 2 already succeeded; split steps with checkpointed absent-key artifacts for failure recovery without double-writing business columns.
- **Metrics** — Emit rows inserted/updated/soft-deleted per run; use them to decide when to invest in incremental CDC vs full compare.

None of these are required for the assessment slice; they are the paths we’d explore once audit tables hit production volume.

## Risk

- **Clock semantics:** “first seen absent” uses the cluster clock at load time. For strict audit alignment with an upstream business timestamp, you might pass a logical “as-of” time in a future API (out of scope here).
- **ALTER TABLE path** assumes Delta + permissions compatible with `ADD COLUMN` on the target path used in tests and Databricks.

## Follow-ups

- Integration test against Unity Catalog table paths if behaviour differs from local `delta.` paths.
- Optional config for deletion column name (with guardrails) if product needs it.
- Document `_deleted_at` in operator runbooks and BI contracts (filter `WHERE _deleted_at IS NULL` for current-state views).

## Bonus — `deploy.yaml` bug

**Bug:** The `plan` job wrote `terraform/tfplan` and uploaded it as an artifact, but `apply` ignored that file and ran `terraform apply -auto-approve` with inline `-var` flags instead. Apply therefore recomputed changes at deploy time rather than executing the plan reviewers saw.

**Production impact:** Operators could approve a PR plan showing one set of catalog/table changes while production received a different outcome — for example if state drifted between jobs, if another merge landed before apply, or if plan and apply ran on different checkouts. Failures might be silent (wrong schema or load_mode metadata) rather than a hard Terraform error.

**Fix:** Download the `tfplan` artifact in `apply` and run `terraform apply -auto-approve tfplan` so apply is bound to the saved plan output.

**Note:** `deploy.yaml`’s header comment says it runs on merges to `main`, but `on: pull_request` means plan/apply run on PRs (good for reviewing Terraform before merge), not automatically after merge — left as-is for this assessment; production would typically use `push` to `main` for post-merge deploy.

## AI assistant usage

Used for implementation scaffolding, tests, and doc alignment with the repo’s patterns.
