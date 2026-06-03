# DK-812 — Deletion-Preserving Load Mode

## Design

- **YAML interface:** domain teams set `refresh.mode: soft_delete` on the model (same pattern as `full` / `full_compare`). They declare **primary keys** on merge key columns with `primary_key: true`, exactly like `full_compare`.
- **Deletion marker:** the engine injects a single nullable **`TIMESTAMP`** column **`_deleted_at`** on the Delta target. It is **not** declared in model YAML — the loader adds it via `ALTER TABLE` when missing so existing tables can migrate without a Terraform column list change for that field.
  - **`NULL`** → row is **active** (present in source, or restored after reappearance).
  - **Non-null** → row is **inactive**; the value is the UTC instant when the row was **first** seen absent from the source (not updated on later runs — idempotency).
- **Runtime behaviour:** two Delta `MERGE` passes:
  1. **Upsert from source** (match on PKs): update all business columns from source, force `_deleted_at = NULL` (covers inserts, updates, and **reappearance** after soft delete).
  2. **Anti-join keys only:** rows in target with no matching source key and `_deleted_at IS NULL` get `_deleted_at` set to “now” once (`whenNotMatchedBySourceUpdate` with that condition).

## Trade-offs

- **Engine-owned column name** (`_deleted_at`) avoids YAML/schema drift and keeps domain YAML minimal; downside is a fixed name if two conventions collide (unlikely with a single platform column).
- **Two MERGE passes** instead of one large expression — simpler to reason about and test; small extra scan cost.
- **Timestamp via SQL literal** in the merge `set` map — avoids passing `Column` objects into Delta’s string-based merge API; microsecond formatting is tied to Spark’s cast rules.

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
