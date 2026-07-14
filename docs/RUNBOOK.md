# Runbook

## Deploy

```bash
# dev, both clouds
git push origin develop                      # CI -> CD (dev) automatically

# qa
git push origin main                         # CI -> CD (qa) automatically

# prod: approve the GitHub Environment gate, or in Jenkins:
#   Build with Parameters -> CLOUD=both, ENVIRONMENT=prod -> approve the input step
```

Manual, one cloud:

```bash
export CLOUD=azure ENVIRONMENT=qa
make tf-plan && make tf-apply
make bundle-deploy
```

## Common incidents

### A CDC stream is behind

1. `dag_streaming_ops` alerts on consumer lag > SLO (300s in prod).
2. Check the replication slot first — an inactive slot means Debezium is down, not slow:
   ```sql
   SELECT slot_name, active, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
   FROM pg_replication_slots;
   ```
3. If `active = false`: restart the Connect task (`POST /connectors/gc-postgres-cdc-prod/restart`).
   If WAL retention is climbing past ~10 GiB, this is now a database availability problem —
   escalate before the disk fills.
4. If the slot is healthy but Spark is behind: the MERGE is the bottleneck. Check for a skewed
   key, and confirm `OPTIMIZE` ran last night.

### A streaming job is in a crash loop

Almost always a poison pill. Look in the DLQ topic, not in the job logs:
```bash
kafka-console-consumer --topic gc-purchase-events-dlq --from-beginning --max-messages 20
```
If the schema changed upstream, the fix is a schema-registry version bump plus a Bronze
`mergeSchema` — not a `failOnDataLoss=false` band-aid.

### Gold is stale

`assert_bronze_freshness` failed, or a hard expectation fired. Check
`silver.quarantine_*` for the last 24 hours — the failed rule names are in `_dq_failed_rules`.

### Small-file explosion

Symptom: query times creep up, file counts climb into the hundreds of thousands. `OPTIMIZE`
was skipped or `autoCompact` was disabled. Run `make maintenance` and check why the nightly
maintenance task did not.

### A training run OOMs on the GPU

In order of likelihood: batch size raised without lowering `prefetch_factor`; `torch.compile`
with `dynamic=True` retracing on ragged batches (`drop_last=True` fixes it); a second process
holding the card (TF without `set_memory_growth`).

## Rollback

- **Workloads**: `databricks bundle deploy -t prod` from the previous tag. Bundles are declarative;
  redeploying an old commit fully restores the previous job definitions.
- **Data**: Delta time travel.
  ```sql
  RESTORE TABLE gamingcommerce_prod.gold.gold_player_360 TO VERSION AS OF 42;
  ```
- **Infrastructure**: `terraform apply` from the previous commit. Never `terraform destroy` in prod.
- **Models**: move the `@champion` alias back to the previous version. No redeploy needed —
  batch inference resolves the alias at run time.
