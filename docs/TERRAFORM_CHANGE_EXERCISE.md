# Terraform Change-Management Exercise

A hands-on task that demonstrates **evolving live infrastructure safely** with
Terraform — the day-2 skill interviewers care about most. You'll make a change,
read the plan, apply it, verify, and roll it back. Two variants show the two
kinds of change: **update-in-place** vs **add a resource**. A third shows a
**replacement** and how to reason about it.

> Run this after the foundation layer is applied (see `docs/RUNBOOK.md` step 2).
> Both tunables are already wired to variables, so each change is a **one-line
> `terraform.tfvars` edit** — you never hand-edit a resource block for a routine change.

---

## Variant A — Update in place (Log Analytics retention 30 → 60)

1. Baseline is `log_retention_days = 30`. Edit `infra/terraform/environments/dev/foundation/terraform.tfvars`:

   ```hcl
   log_retention_days = 60
   ```

2. Plan and **read the diff** before doing anything:

   ```bash
   cd infra/terraform/environments/dev/foundation
   terraform plan -out=change.tfplan
   ```

   Expect exactly one change, marked `~` (update in place):

   ```
   ~ resource "azurerm_log_analytics_workspace" "law" {
       ~ retention_in_days = 30 -> 60
         # (all other attributes unchanged)
     }
   Plan: 0 to add, 1 to change, 0 to destroy.
   ```

   The `~` prefix (not `-/+`) is the point: the resource is **modified**, not
   recreated — no data loss, no new ID.

3. Apply the *saved* plan (so you apply exactly what you reviewed):

   ```bash
   terraform apply change.tfplan
   ```

4. Verify:

   ```bash
   az monitor log-analytics workspace show \
     -g "$(terraform output -raw resource_group)" \
     -n "law-ecomlakedev" --query retentionInDays -o tsv   # -> 60
   ```

5. Roll back: set it to `30`, `plan`, `apply`. Same in-place path in reverse.

## Variant B — Add a resource (new ADLS container)

1. Edit `terraform.tfvars`:

   ```hcl
   extra_containers = ["sandbox"]
   ```

2. `terraform plan` shows **1 to add** — a new
   `azurerm_storage_data_lake_gen2_filesystem.containers["sandbox"]`, and
   **nothing else touched** (the `for_each` keys the existing containers by name,
   so adding one doesn't disturb the others).

3. Apply, verify the container exists, then remove `"sandbox"` and apply again to
   delete just that one.

   > Interview note: this is why `for_each` (keyed by a stable value) beats
   > `count` (keyed by list index) — inserting/removing an element with `count`
   > can ripple and recreate unrelated resources; `for_each` doesn't.

## Variant C — Understand a replacement (don't run on real data)

Some attributes can't change in place. Example: renaming the storage account, or
changing a property Azure marks `ForceNew`. The plan shows `-/+ destroy and then
create` and a `# forces replacement` note on the offending line.

- **Never** blind-apply a replacement on a stateful resource (a storage account
  holding your lake) — you'd lose data.
- To intentionally recreate one specific resource:
  `terraform apply -replace="azurerm_databricks_access_connector.uc"`.
- Prefer additive migrations (create new → move data → cut over → destroy old).

---

## What to say in the interview

- **Plan before apply, always.** `-out=plan` then `apply plan` guarantees you run
  exactly what you reviewed — no drift between review and execution.
- **In-place (`~`) vs replacement (`-/+`).** I read the plan for `forces
  replacement` on stateful resources; a surprise replacement is a stop-and-think.
- **Change via variables, not by editing resources.** Routine changes are a
  reviewed `tfvars` diff in a PR; CI runs `plan` on the PR
  (`.github/workflows/terraform.yml`), a human approves, `apply` runs on merge.
- **State is the source of truth.** Terraform diffs desired config against state;
  I keep state in a locked, versioned backend so two people can't corrupt it, and
  I use `terraform plan` to detect drift if someone changed a resource by hand.
- **Rollback = revert the variable and re-apply** for in-place changes; for
  replacements I plan an additive migration instead.
- **Blast radius.** This repo splits `foundation` and `platform` into separate
  states, so a change in one can't corrupt the other.
