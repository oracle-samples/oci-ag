# Release notes: 1.2

## Permission-assignment event support

DFA now supports permission-assignment event versions 1.0 and 2.0 for both file and stream ingestion. The version is read from the event header; other entity types continue to accept version 1.0 only.

- Version 1.0 supports the nested `add` and `remove` payload format.
- Version 2.0 supports the flat permission-assignment array format. Delete payloads use the same array format and may contain only the assignment `id`.
- Version 2.0 `created` and `lastModified` values are stored in `CREATED_ON` and `UPDATED_ON`.
- For both versions, `customAttributes` is stored in `ATTRIBUTES`.
- Permission-assignment time-series rows may have a null assignment ID. State rows without an assignment ID are skipped.
- Streamed rows retain `EVENT_OBJECT_TYPE` and `OPERATION_TYPE`, including delete operations.

## State-table event ordering

State-table updates are applied only when the incoming `EVENT_TIMESTAMP` is newer than the stored value. Permission-assignment state deletes have the same newer-event safeguard.

## Schema changes

Permission-assignment state and time-series tables add these columns:

- `CREATED_ON`
- `UPDATED_ON`
- `STATUS`
- `ACCOUNT_STATUS`

`ASSIGNMENT_ATTRIBUTES` is removed. The permission-assignment state-table unique constraint is now based on `ASSIGNMENT_ID`, `SERVICE_INSTANCE_ID`, and `TENANCY_ID`; `ASSIGNMENT_ID` remains nullable in the time-series table and is non-null in the state table.

### Upgrade requirement

Existing deployments require a manual database migration before deploying version 1.2. Add the new columns, migrate any data that must be retained from `ASSIGNMENT_ATTRIBUTES` into `ATTRIBUTES`, resolve null or duplicate state-table assignment IDs, replace the existing permission-assignment state unique constraint and backing index, and then remove `ASSIGNMENT_ATTRIBUTES` if appropriate. DFA creates the new schema for fresh installations but does not alter existing tables or indexes automatically.

```
-- verify assignment_id is not null
SELECT COUNT(*) AS NULL_ASSIGNMENT_IDS
FROM DFA_USER.PERMISSION_ASSIGNMENT_STATE
WHERE ASSIGNMENT_ID IS NULL;

-- if null delete
DELETE FROM DFA_USER.PERMISSION_ASSIGNMENT_STATE
WHERE ASSIGNMENT_ID IS NULL;

-- update
ALTER TABLE DFA_USER.PERMISSION_ASSIGNMENT_STATE
MODIFY (ASSIGNMENT_ID NOT NULL);


ALTER TABLE DFA_USER.PERMISSION_ASSIGNMENT_STATE
ADD (STATUS VARCHAR2(256), ACCOUNT_STATUS VARCHAR2(256), CREATED_ON NUMBER, UPDATED_ON NUMBER);
ALTER TABLE DFA_USER.PERMISSION_ASSIGNMENT_TS
ADD (STATUS VARCHAR2(256), ACCOUNT_STATUS VARCHAR2(256), CREATED_ON NUMBER, UPDATED_ON NUMBER);

ALTER TABLE DFA_USER.PERMISSION_ASSIGNMENT_STATE
DROP COLUMN ASSIGNMENT_ATTRIBUTES;
ALTER TABLE DFA_USER.PERMISSION_ASSIGNMENT_TS
DROP COLUMN ASSIGNMENT_ATTRIBUTES;

-- verify there are no duplicates
SELECT ASSIGNMENT_ID, COUNT(*)
FROM DFA_USER.PERMISSION_ASSIGNMENT_STATE
GROUP BY ASSIGNMENT_ID
HAVING COUNT(*) > 1;

-- if duplicate rows exist, delete

-- update unique index

DROP INDEX DFA_USER.DFA_UNQ_PA_ST_CONST;

CREATE UNIQUE INDEX DFA_USER.DFA_UNQ_PA_ST_CONST
ON DFA_USER.PERMISSION_ASSIGNMENT_STATE (
    ASSIGNMENT_ID,
    SERVICE_INSTANCE_ID,
    TENANCY_ID
);

ALTER TABLE DFA_USER.PERMISSION_ASSIGNMENT_STATE
ADD CONSTRAINT DFA_UNQ_PA_ST_CONST
UNIQUE (
    ASSIGNMENT_ID,
    SERVICE_INSTANCE_ID,
    TENANCY_ID
)
USING INDEX DFA_USER.DFA_UNQ_PA_ST_CONST
ENABLE;
```
