/* =============================================================================
   Synovia Fusion - Enterprise Audit Table
   =============================================================================
   Table: EXC.Process_Transaction_Log

   Purpose:
     - Generic Process / SubProcess / Step audit ledger
     - Links to Execution Framework Run_ID / Txn_ID
     - Stores STARTED/PASS/FAIL/WARN/SKIPPED with severity classification
     - Supports rich Details_JSON (ISJSON validated)
     - Intended for ALL pipeline scripts (EPOS and beyond)

   Notes:
     - Create table once in the target database (PRD/UAT/etc.)
     - Grant INSERT/UPDATE to the runtime identity (SQL Agent / service principal)
   ============================================================================= */

-- Ensure schema exists
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'EXC')
BEGIN
    EXEC('CREATE SCHEMA EXC');
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = 'EXC' AND t.name = 'Process_Transaction_Log'
)
BEGIN
    CREATE TABLE EXC.Process_Transaction_Log
    (
        Log_ID              BIGINT IDENTITY(1,1) NOT NULL
            CONSTRAINT PK_EXC_Process_Transaction_Log PRIMARY KEY CLUSTERED,

        -- Linkage (Execution Framework)
        Run_ID              BIGINT               NOT NULL,
        Txn_ID              BIGINT               NULL,
        Correlation_ID      UNIQUEIDENTIFIER     NULL,

        -- Classification (stable)
        System_Name         NVARCHAR(100)        NOT NULL,
        Environment         NVARCHAR(50)         NOT NULL,
        Orchestrator        NVARCHAR(32)         NULL,

        Process_Name        NVARCHAR(200)        NOT NULL,
        SubProcess_Name     NVARCHAR(200)        NULL,
        Step_Name           NVARCHAR(300)        NOT NULL,

        Step_Code           NVARCHAR(100)        NOT NULL,
        Step_Category       NVARCHAR(50)         NOT NULL, -- VALIDATION/DB/FILESYSTEM/CONFIG/SECURITY/TRANSFORM/IO/ACTION

        Severity_Level      NVARCHAR(20)         NOT NULL, -- INFO/WARN/ERROR/CRITICAL
        Execution_Status    NVARCHAR(20)         NOT NULL, -- STARTED/PASS/FAIL/WARN/SKIPPED

        -- Metrics
        Rows_Read           INT                  NULL,
        Rows_Affected       INT                  NULL,
        Bytes_Processed     BIGINT               NULL,

        -- Business references
        Source_Reference    NVARCHAR(500)        NULL,     -- e.g. filename
        Reference_Number    NVARCHAR(200)        NULL,     -- e.g. RAW File_ID
        Idempotency_Key     CHAR(64)             NULL,     -- e.g. sha256

        -- Human + machine details
        Message             NVARCHAR(2000)       NULL,
        Error_Message       NVARCHAR(4000)       NULL,
        Details_JSON        NVARCHAR(MAX)        NULL,

        -- Timing
        Start_UTC           DATETIME2(3) NOT NULL
            CONSTRAINT DF_EXC_Process_Transaction_Log_Start DEFAULT SYSUTCDATETIME(),
        End_UTC             DATETIME2(3) NULL,

        Duration_ms AS (
            CASE WHEN End_UTC IS NULL THEN NULL
                 ELSE DATEDIFF(MILLISECOND, Start_UTC, End_UTC)
            END
        ) PERSISTED,

        Created_UTC         DATETIME2(3) NOT NULL
            CONSTRAINT DF_EXC_Process_Transaction_Log_Created DEFAULT SYSUTCDATETIME(),
        Created_By          NVARCHAR(128) NOT NULL
            CONSTRAINT DF_EXC_Process_Transaction_Log_CreatedBy DEFAULT SUSER_SNAME()
    );

    -- Constraints (guard rails for audit quality)
    ALTER TABLE EXC.Process_Transaction_Log
        ADD CONSTRAINT CK_EXC_Process_Transaction_Log_Status
        CHECK (Execution_Status IN ('STARTED','PASS','FAIL','WARN','SKIPPED'));

    ALTER TABLE EXC.Process_Transaction_Log
        ADD CONSTRAINT CK_EXC_Process_Transaction_Log_Severity
        CHECK (Severity_Level IN ('INFO','WARN','ERROR','CRITICAL'));

    ALTER TABLE EXC.Process_Transaction_Log
        ADD CONSTRAINT CK_EXC_Process_Transaction_Log_DetailsJson
        CHECK (Details_JSON IS NULL OR ISJSON(Details_JSON) = 1);

    -- Indexes
    CREATE INDEX IX_EXC_Process_Transaction_Log_Run
    ON EXC.Process_Transaction_Log (Run_ID, Log_ID);

    CREATE INDEX IX_EXC_Process_Transaction_Log_Txn
    ON EXC.Process_Transaction_Log (Txn_ID, Log_ID)
    INCLUDE (Execution_Status, Severity_Level, Step_Code, Step_Category, Start_UTC, End_UTC);

    -- Fast “show me failures” query
    CREATE INDEX IX_EXC_Process_Transaction_Log_Failures
    ON EXC.Process_Transaction_Log (Execution_Status, Start_UTC)
    INCLUDE (Run_ID, Txn_ID, Process_Name, Step_Code, Error_Message)
    WHERE Execution_Status IN ('FAIL','WARN');

END
GO

/* Optional compatibility synonyms
   - Lets existing/older scripts write to EXC.Process_Steps or EXC.EPOS_Process_Steps
     without changing their code.
*/

IF NOT EXISTS (
    SELECT 1
    FROM sys.synonyms sn
    JOIN sys.schemas s ON s.schema_id = sn.schema_id
    WHERE s.name = 'EXC' AND sn.name = 'Process_Steps'
)
BEGIN
    CREATE SYNONYM EXC.Process_Steps FOR EXC.Process_Transaction_Log;
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.synonyms sn
    JOIN sys.schemas s ON s.schema_id = sn.schema_id
    WHERE s.name = 'EXC' AND sn.name = 'EPOS_Process_Steps'
)
BEGIN
    CREATE SYNONYM EXC.EPOS_Process_Steps FOR EXC.Process_Transaction_Log;
END
GO
