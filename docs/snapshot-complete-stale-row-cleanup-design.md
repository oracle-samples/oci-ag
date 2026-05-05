# Snapshot Completion and Stale Row Cleanup Design

## Overview

Snapshot file ingestion updates state tables in batches. A snapshot is complete only after all expected snapshot batches have been loaded and a completion marker has been received. At that point, rows in the target state table that are older than the completed snapshot can be considered stale and removed.

The implementation is intentionally non-blocking for normal batch ingestion. Each data batch records its completion in a helper table. The completion marker later checks whether all expected batches have finished, acquires a cleanup lock, deletes stale rows, and clears the helper records for that snapshot.

## Goals

- Track completed snapshot batches for each state-table entity.
- Use the completion marker's `numOfBatches` header as the source of truth for how many batches must finish.
- Delete stale state rows only after every expected snapshot batch has completed.
- Scope cleanup by entity type, snapshot ID, tenancy ID, and service instance ID.
- Allow multiple completion-marker executions without duplicate cleanup work.
- Avoid concurrent finalizers deleting the same table scope at the same time.
- Retry cleanup on Oracle deadlock errors where the operation is safe to retry.

## Non-Goals

- This design does not apply to time-series table ingestion.
- This design does not derive the required batch count from the helper table.
- This design does not delete stale rows for normal `UPDATE` or `DELETE` events. It is specific to state-table snapshot `CREATE` ingestion.

## Inputs

Snapshot metadata is read from file event headers by `FileTransformer`:

- `messageType`: entity type, such as `IDENTITY` or `ACCESS_BUNDLE`.
- `operation`: must be `CREATE` for snapshot tracking and cleanup.
- `eventTime`: batch event timestamp.
- `correlationId`: snapshot ID. If missing, the object name is used as a fallback snapshot ID.
- `status`: completion marker status. `COMPLETED` identifies a completion marker when the file has no raw data events.
- `numOfBatches`: expected number of snapshot data batches.
- `tenancyId`: cleanup tenant scope.
- `serviceInstanceId`: cleanup service instance scope.

## Helper Table

Completed snapshot batches are stored in `SNAPSHOT_BATCH_TRACKER`.

Columns:

- `ENTITY_TYPE`
- `TENANCY_ID`
- `SERVICE_INSTANCE_ID`
- `SNAPSHOT_ID`
- `BATCH_ID`
- `UPDATED_AT`

The primary key is:

- `ENTITY_TYPE`
- `TENANCY_ID`
- `SERVICE_INSTANCE_ID`
- `SNAPSHOT_ID`
- `BATCH_ID`

`ENTITY_TYPE` is derived from the target table name by stripping `_STATE` or `_TS`. Missing tenancy or service instance values are normalized to `"-"` for tracker-table scope consistency.

The tracker primary key is also enforced idempotently when snapshot tracking runs. This covers existing deployments where `SNAPSHOT_BATCH_TRACKER` may have been created before the primary key was introduced.

## Normal Snapshot Batch Flow

For a non-timeseries `CREATE` file with valid object type and at least one raw event:

1. `FileTransformer.extract_data()` reads headers and raw events.
2. `FileTransformer.transform_data()` creates prepared events.
3. `FileTransformer.load_data()` writes prepared events to the state table through the appropriate query builder.
4. After successful load, `register_snapshot_batch_completed()` inserts one tracker row for the batch.
5. `UPDATED_AT` is set to the batch event timestamp converted to UTC and formatted as `DD-MON-RR HH24:MI:SS.FF6`.

Normal data batches do not trigger stale-row cleanup.

## Completion Marker Flow

A completion marker is identified when all of the following are true:

- The file has zero raw events.
- `numOfBatches` is present and parseable.
- `status` is `COMPLETED`, case-insensitive after trimming.
- The file is a non-timeseries `CREATE` event for a valid object type.

When a completion marker is loaded:

1. `FileTransformer.load_data()` creates a query builder for the target entity with no events.
2. No state-table event rows are written because the marker contains no raw events.
3. `finalize_snapshot_cleanup_if_ready()` is called with snapshot ID, expected batch count, tenancy ID, and service instance ID.

## Finalization Algorithm

`finalize_snapshot_cleanup_if_ready()` performs the cleanup transaction.

1. If `numOfBatches` is missing, return immediately.
2. Count completed tracker rows for the entity, snapshot, tenancy, and service instance.
3. If completed count is less than `numOfBatches`, roll back the read transaction and return.
4. Acquire a cleanup lock by selecting the deterministic tracker row with `MIN(BATCH_ID)` for the same scope `FOR UPDATE NOWAIT`.
5. If the lock is busy (`ORA-00054`), roll back and return.
6. Read the earliest completed batch timestamp with `MIN(UPDATED_AT)`.
7. If there is no timestamp, roll back and return.
8. Delete and commit stale rows from the target state table where `EVENT_TIMESTAMP` is earlier than the earliest completed batch timestamp, scoped by tenancy and service instance when present.
9. Delete and commit all tracker rows for the completed snapshot scope.

The earliest batch completion timestamp is used as the cleanup boundary so rows loaded by any batch in the new snapshot are not removed.

The stale-row delete and tracker cleanup are committed separately. This keeps large state-table deletes for high-volume entities such as `IDENTITY` and `PERMISSION_ASSIGNMENT` from holding a long transaction open while tracker rows are cleared. If tracker cleanup fails after the stale-row delete commits, a later completion-marker retry can safely re-run the stale-row delete and then clear the tracker rows.

## Stale Row Delete SQL

The stale-row delete uses the target table managed by the query builder:

```sql
DELETE FROM "<STATE_TABLE>"
WHERE "EVENT_TIMESTAMP" < TO_TIMESTAMP(:COMPLETION_TIMESTAMP, 'DD-MON-RR HH24:MI:SS.FF6')
```

Optional scope predicates are added when values are present:

```sql
AND "TENANCY_ID" = :TENANCY_ID
AND "SERVICE_INSTANCE_ID" = :SERVICE_INSTANCE_ID
```

Supported cleanup timestamp input formats are:

- `DD-MON-RR HH24:MI:SS.FF6`
- `DD-MON-RR HH:MI:SS.FF6 AM`

The timestamp is normalized to `DD-MON-RR HH24:MI:SS.FF6` before binding.

## Concurrency Control

Multiple workers may process completion markers or retry finalization for the same snapshot. Cleanup must run at most once for a given entity/snapshot/scope.

The cleanup lock targets a deterministic tracker row:

```sql
SELECT BATCH_ID
FROM <schema>.SNAPSHOT_BATCH_TRACKER
WHERE ENTITY_TYPE = :ENTITY_TYPE
  AND TENANCY_ID = :TENANCY_ID
  AND SERVICE_INSTANCE_ID = :SERVICE_INSTANCE_ID
  AND SNAPSHOT_ID = :SNAPSHOT_ID
  AND BATCH_ID = (
      SELECT MIN(BATCH_ID)
      FROM <schema>.SNAPSHOT_BATCH_TRACKER
      WHERE ENTITY_TYPE = :ENTITY_TYPE
        AND TENANCY_ID = :TENANCY_ID
        AND SERVICE_INSTANCE_ID = :SERVICE_INSTANCE_ID
        AND SNAPSHOT_ID = :SNAPSHOT_ID
  )
FOR UPDATE NOWAIT
```

Using `MIN(BATCH_ID)` ensures all concurrent finalizers contend for the same row. If a worker cannot acquire the lock, it exits without failing the ingestion flow.

## Retry Behavior

Cleanup treats Oracle database and interface errors as transient cleanup errors and rolls back/closes the connection before raising or retrying.

`ORA-00060` deadlocks during cleanup are retried because the cleanup is idempotent for the same entity/snapshot/scope:

- Maximum attempts: `STALE_ROW_DELETE_MAX_ATTEMPTS`
- Delay between attempts: `STALE_ROW_DELETE_RETRY_DELAY_SECONDS`

Other retryable cleanup errors are rolled back/closed and re-raised.

## Idempotency

The design is idempotent across repeated completion-marker handling:

- If not all batches have completed, finalization returns without deleting rows.
- If another worker is already cleaning up, finalization returns without deleting rows.
- If cleanup has already completed, tracker rows for the snapshot scope have been removed, so later finalizers cannot acquire a tracker-row lock and do not repeat cleanup.
- The stale-row delete is scoped and timestamp-based, so retrying after rollback is safe.

## Observability

The implementation logs:

- Snapshot cleanup deferral when completed batch count is below the required count.
- Snapshot cleanup deferral when another worker holds the cleanup lock.
- Stale-row delete start, including target table and timestamp boundary.
- Tracker-row cleanup completion.
- Deadlock retry attempts for `ORA-00060`.

## Test Coverage

The current tests cover:

- Header parsing for completion marker metadata.
- Tracking normal snapshot batches.
- Ensuring normal batches do not finalize cleanup.
- Completion marker finalization call behavior.
- Tracker count query scope.
- Earliest batch timestamp formatting.
- Deterministic cleanup lock query.
- Deferral when required batch count is not met.
- Deferral when the cleanup lock is busy.
- Successful stale-row delete and tracker cleanup.
- Rollback when no earliest timestamp is found.
- Rollback/close behavior for transient tracker and delete errors.
- Retry behavior for `ORA-00060`.
