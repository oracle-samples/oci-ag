# Release notes: 1.3

## System-event support

DFA now supports `SYSTEM_EVENTS` event version `1.0` with `CREATE` envelopes.
The existing audit handler processes both audit and system events through the
same OCI Function and Service Connector. Audit events continue to use
`AUDIT_EVENTS`; system events are appended to the new `SYSTEM_EVENTS` table.
Generic stream and file handlers skip system events to prevent duplicate ingestion.

- Source event IDs, types, versions, origins, actions, and request IDs are stored in separate columns.
- `SOURCE_EVENT_TIMESTAMP` preserves the source `eventTimestamp` string, including nanoseconds.
- `EVENT_TIMESTAMP` stores the data-enablement envelope time using DFA's existing timestamp conversion.
- `DATA` preserves the complete nested source payload as JSON in a CLOB, including target-operation `value`, `oldValue`, `newValue`, and `changes` when present.
- System events have no state table. Nested target-operation updates are stored as new event rows.

The Python module/class is now `event_transformer.py` / `EventTransformer`.
The deployed audit function name, `DFA_FUNCTION_NAME=audit`, connector name, and
`dfa_audit_transformer` consumer/offset-tracking identifier remain unchanged.
Both event types share the audit function's capacity and retry boundary.

## Upgrade from 1.2

Existing customers do not need to rerun the full installer or create a new
function or connector. Create the table and index below **before** deploying the
1.3 image to the existing audit function. Customers upgrading from an earlier
version must also complete the [1.2 database migration](release-notes-1.2.md).

### 1. Create the table and index

Connect to the DFA Autonomous Database as the DFA schema owner, or as an
administrator with permission to create objects in that schema. The examples
use `DFA_USER`; replace it with the schema configured by `DFA_ADW_DFA_SCHEMA`
if different.

Check whether the objects already exist:

```sql
SELECT TABLE_NAME
FROM ALL_TABLES
WHERE OWNER = 'DFA_USER' AND TABLE_NAME = 'SYSTEM_EVENTS';

SELECT INDEX_NAME, TABLE_NAME, STATUS
FROM ALL_INDEXES
WHERE OWNER = 'DFA_USER' AND INDEX_NAME = 'DFA_SE_ET_IDX';
```

Run each creation statement only if its object is absent. If an object already
exists, verify its definition against the SQL below; do not drop the table or
its data. These definitions match the DFA 1.3 installer.

```sql
CREATE TABLE DFA_USER.SYSTEM_EVENTS (
    SOURCE_EVENT_TIMESTAMP    VARCHAR2(4000),
    SOURCE_EVENT_TYPE         VARCHAR2(4000),
    SOURCE_EVENT_ID           VARCHAR2(4000),
    SOURCE_EVENT_TYPE_VERSION VARCHAR2(4000),
    SOURCE_ORIGIN             VARCHAR2(4000),
    SOURCE_ACTION             VARCHAR2(4000),
    SOURCE_REQUEST_ID         VARCHAR2(4000),
    DATA                      CLOB,
    TENANCY_ID                VARCHAR2(4000),
    SERVICE_INSTANCE_ID       VARCHAR2(4000),
    EVENT_OBJECT_TYPE         VARCHAR2(4000),
    OPERATION_TYPE            VARCHAR2(4000),
    EVENT_TIMESTAMP          TIMESTAMP WITH TIME ZONE
);

CREATE INDEX DFA_USER.DFA_SE_ET_IDX
ON DFA_USER.SYSTEM_EVENTS (
    SYS_EXTRACT_UTC("EVENT_TIMESTAMP"),
    SERVICE_INSTANCE_ID,
    TENANCY_ID
);
```

The index is non-unique and supports event-time queries scoped to service
instance and tenancy. Verify its status and column order:

```sql
SELECT i.INDEX_NAME, i.STATUS, c.COLUMN_POSITION, c.COLUMN_NAME
FROM ALL_INDEXES i
JOIN ALL_IND_COLUMNS c
  ON c.INDEX_OWNER = i.OWNER AND c.INDEX_NAME = i.INDEX_NAME
WHERE i.OWNER = 'DFA_USER' AND i.INDEX_NAME = 'DFA_SE_ET_IDX'
ORDER BY c.COLUMN_POSITION;
```

Expect `VALID`, a generated column for the UTC expression in position 1, and
`SERVICE_INSTANCE_ID`, `TENANCY_ID` in positions 2 and 3. Verify the expression:

```sql
SELECT COLUMN_POSITION, COLUMN_EXPRESSION
FROM ALL_IND_EXPRESSIONS
WHERE INDEX_OWNER = 'DFA_USER' AND INDEX_NAME = 'DFA_SE_ET_IDX'
ORDER BY COLUMN_POSITION;
```

Position 1 should contain `SYS_EXTRACT_UTC("EVENT_TIMESTAMP")`. If the index was
already created using the earlier plain timestamp definition, drop only the
index and recreate it with the definition above:

```sql
DROP INDEX DFA_USER.DFA_SE_ET_IDX;
```

The installer checks for an existing index by name; it does not replace its
definition automatically.

### 2. Deploy the 1.3 image

Build and publish the DFA 1.3 image using the existing deployment procedure,
then update the existing audit function to that image. Keep its
`DFA_FUNCTION_NAME=audit` configuration and existing stream connector. No new
concurrency setting or consumer identifier is required.

After the source publishes system events to the configured DFA stream, verify
that the audit function processes them and rows appear in the new table:

```sql
SELECT SOURCE_EVENT_ID, SOURCE_EVENT_TYPE, SOURCE_ACTION,
       SOURCE_EVENT_TIMESTAMP, EVENT_TIMESTAMP
FROM DFA_USER.SYSTEM_EVENTS
ORDER BY EVENT_TIMESTAMP DESC
FETCH FIRST 10 ROWS ONLY;
```

## Fresh installations

The 1.3 installer creates `SYSTEM_EVENTS` and `DFA_SE_ET_IDX` alongside
`AUDIT_EVENTS`, independently of `CREATE_TIME_SERIES`. The existing audit
function and connector handle both event types.
