-- =============================================================
--  Procedure : [EXC].[usp_EPOS_Generate_Weekly_Orders_Step6]
--  Database  : Fusion_EPOS_Production
--  Exported  : 2026-03-30 02:45:06
--  Created   : 2026-02-12 18:40:52.580000
--  Modified  : 2026-02-12 19:21:02.280000
-- =============================================================
USE [Fusion_EPOS_Production]
GO

/* =====================================================================================
   Synovia Fusion – Step 6: Stage Weekly Orders into INT.SalesOrderStage
   -------------------------------------------------------------------------------------
   Source:
     - CTL.EPOS_Header    where Integration_Status = 'READY'
     - CTL.EPOS_Line_Item where Integration_Status = 'READY'

   Target:
     - INT.SalesOrderStage with Integration_Status = 'STAGED'

   Status updates:
     - CTL.EPOS_Line_Item.Integration_Status   READY -> SO_STAGED   (only those staged)
     - CTL.EPOS_Header.Integration_Status      READY -> SO_STAGED   (only when no READY lines remain)

   Logging:
     - EXC.Process_Transaction_Log (no writes to Duration_ms)

   Hard rules (enforced in ALL cases):
     - dataAreaId        = 'jbro'
     - SalesUnitSymbol   = 'EA'

   Notes:
     - Aggregates to one staged line per (CustomersOrderReference, ItemNumber)
       even if CTL still contains duplicates (belt + braces).
     - NOT EXISTS prevents duplicates in INT.
     - “Healing”: if INT already has the row (STAGED), it is forced to jbro/EA so reruns are safe.
   ===================================================================================== */
CREATE   PROCEDURE [EXC].[usp_EPOS_Generate_Weekly_Orders_Step6]
      @Environment           nvarchar(20)
    , @System_Name           nvarchar(50)
    , @Run_ID                bigint
    , @Txn_ID                bigint             = NULL
    , @Correlation_ID        uniqueidentifier   = NULL

    , @Year                  int                = NULL
    , @Week                  int                = NULL
    , @File_ID               int                = NULL

    , @DataAreaId            nvarchar(10)       = N'jbro'  -- ignored (hard-enforced below)
    , @SalesUnitSymbol       nvarchar(10)       = N'EA'    -- ignored (hard-enforced below)
    , @RequestedShippingDate date               = NULL     -- if NULL defaults to today (UTC)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
          @Process_Name     nvarchar(100) = N'EPOS_Generate_Weekly_Orders'
        , @SubProcess_Name  nvarchar(100) = N'Step6'
        , @StartUTC         datetime2(3)
        , @EndUTC           datetime2(3)
        , @Idem             nvarchar(200)
        , @Details          nvarchar(max)
        , @Msg              nvarchar(2000);

    IF @Correlation_ID IS NULL
        SET @Correlation_ID = NEWID();

    IF @RequestedShippingDate IS NULL
        SET @RequestedShippingDate = CONVERT(date, SYSUTCDATETIME());

    /* ---------------------------------------------------------------------
       HARD ENFORCE (ALL CASES) - do not allow caller overrides
    --------------------------------------------------------------------- */
    SET @DataAreaId      = N'jbro';
    SET @SalesUnitSymbol = N'EA';

    /* -----------------------------
       STEP6_START log
    ----------------------------- */
    SET @StartUTC = SYSUTCDATETIME();
    SET @EndUTC   = @StartUTC;
    SET @Idem     = CONCAT(N'S6_START;Y=', COALESCE(CONVERT(varchar(20),@Year),N'*'),
                           N';W=', COALESCE(CONVERT(varchar(20),@Week),N'*'),
                           N';F=', COALESCE(CONVERT(varchar(20),@File_ID),N'*'));

    INSERT INTO EXC.Process_Transaction_Log
    (
        Run_ID, Txn_ID, Correlation_ID,
        System_Name, Environment,
        Process_Name, SubProcess_Name,
        Step_Name, Step_Code, Step_Category,
        Severity_Level, Execution_Status,
        Rows_Read, Rows_Affected, Bytes_Processed,
        Source_Reference, Reference_Number, Idempotency_Key,
        Message, Error_Message, Details_JSON,
        Start_UTC, End_UTC
    )
    VALUES
    (
        @Run_ID, @Txn_ID, @Correlation_ID,
        @System_Name, @Environment,
        @Process_Name, @SubProcess_Name,
        N'Start Step 6', N'STEP6_START', N'RUN_CONTROL',
        N'INFO', N'SUCCESS',
        0, 0, NULL,
        N'ALL', NULL, @Idem,
        N'Step 6 started', NULL, NULL,
        @StartUTC, @EndUTC
    );

    /* -----------------------------
       Resolve scope (Year/Week/File)
    ----------------------------- */
    DECLARE @ScopeYear int = @Year, @ScopeWeek int = @Week, @ScopeFile int = @File_ID;

    IF @ScopeYear IS NULL OR @ScopeWeek IS NULL
    BEGIN
        SELECT TOP (1)
            @ScopeYear = h.[Year],
            @ScopeWeek = h.[Week]
        FROM CTL.EPOS_Header h
        WHERE h.Integration_Status = 'READY'
        ORDER BY h.[Year] DESC, h.[Week] DESC;
    END

    IF @ScopeYear IS NULL OR @ScopeWeek IS NULL
    BEGIN
        -- Nothing eligible to stage
        SET @StartUTC = SYSUTCDATETIME();
        SET @EndUTC   = @StartUTC;
        SET @Idem     = N'S6_SCOPE_NONE';

        INSERT INTO EXC.Process_Transaction_Log
        (
            Run_ID, Txn_ID, Correlation_ID,
            System_Name, Environment,
            Process_Name, SubProcess_Name,
            Step_Name, Step_Code, Step_Category,
            Severity_Level, Execution_Status,
            Rows_Read, Rows_Affected,
            Source_Reference, Idempotency_Key,
            Message, Error_Message,
            Start_UTC, End_UTC
        )
        VALUES
        (
            @Run_ID, @Txn_ID, @Correlation_ID,
            @System_Name, @Environment,
            @Process_Name, @SubProcess_Name,
            N'Resolve scope', N'S6_SCOPE', N'RUN_CONTROL',
            N'INFO', N'SUCCESS',
            0, 0,
            N'CTL.EPOS_Header', @Idem,
            N'No READY CTL headers found. Nothing to stage.', NULL,
            @StartUTC, @EndUTC
        );

        SELECT
              @Run_ID          AS Run_ID
            , @Txn_ID          AS Txn_ID
            , @Correlation_ID  AS Correlation_ID
            , NULL             AS [Year]
            , NULL             AS [Week]
            , NULL             AS File_ID
            , 0                AS Source_Lines
            , 0                AS INT_RowsInserted
            , 0                AS INT_RowsHealed
            , 0                AS CTL_LinesUpdated
            , 0                AS CTL_HeadersUpdated
            , @RequestedShippingDate AS RequestedShippingDate
            , @DataAreaId      AS DataAreaId_Enforced
            , @SalesUnitSymbol AS SalesUnitSymbol_Enforced;

        RETURN 0;
    END

    IF @ScopeFile IS NULL
    BEGIN
        SELECT @ScopeFile = MAX(h.Source_File_ID)
        FROM CTL.EPOS_Header h
        WHERE h.[Year] = @ScopeYear
          AND h.[Week] = @ScopeWeek
          AND h.Integration_Status = 'READY';
        -- ok if still NULL (means mixed/unknown file id) - we treat as "no file filter"
    END

    SET @StartUTC = SYSUTCDATETIME();

    SET @Details = CONCAT(
        N'{"scope_year":', @ScopeYear,
        N',"scope_week":', @ScopeWeek,
        N',"scope_file_id":', COALESCE(CONVERT(varchar(30),@ScopeFile), N'null'),
        N',"dataAreaId":"', REPLACE(@DataAreaId,'"','\"'),
        N'","salesUnitSymbol":"', REPLACE(@SalesUnitSymbol,'"','\"'),
        N'","requestedShippingDate":"', CONVERT(varchar(10),@RequestedShippingDate,120),
        N'"}'
    );

    SET @EndUTC = SYSUTCDATETIME();
    SET @Idem   = CONCAT(N'S6_SCOPE;Y=',@ScopeYear,N';W=',@ScopeWeek,N';F=',COALESCE(CONVERT(varchar(20),@ScopeFile),N'*'));

    INSERT INTO EXC.Process_Transaction_Log
    (
        Run_ID, Txn_ID, Correlation_ID,
        System_Name, Environment,
        Process_Name, SubProcess_Name,
        Step_Name, Step_Code, Step_Category,
        Severity_Level, Execution_Status,
        Rows_Read, Rows_Affected,
        Source_Reference, Idempotency_Key,
        Message, Error_Message, Details_JSON,
        Start_UTC, End_UTC
    )
    VALUES
    (
        @Run_ID, @Txn_ID, @Correlation_ID,
        @System_Name, @Environment,
        @Process_Name, @SubProcess_Name,
        N'Resolve staging scope', N'S6_SCOPE', N'RUN_CONTROL',
        N'INFO', N'SUCCESS',
        0, 0,
        N'CTL.EPOS_Header', @Idem,
        N'Scope resolved for Step 6', NULL, @Details,
        @StartUTC, @EndUTC
    );

    /* -----------------------------
       Build source set (aggregated)
    ----------------------------- */
    IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

    CREATE TABLE #src
    (
        CustomersOrderReference        varchar(50)   NOT NULL,
        OrderingCustomerAccountNumber  varchar(50)   NULL,
        ItemNumber                     varchar(50)   NOT NULL,
        OrderedSalesQuantity           decimal(18,4) NOT NULL,
        LineAmount                     decimal(18,4) NULL,
        LineNumber                     int           NOT NULL
    );

    DECLARE
          @RowsSource     int = 0
        , @RowsInserted   int = 0
        , @RowsHealed     int = 0
        , @RowsUpdLines   int = 0
        , @RowsUpdHdrs    int = 0;

    BEGIN TRY
        DECLARE @TxnStart datetime2(3) = SYSUTCDATETIME();

        BEGIN TRAN;

        /* Populate #src (aggregated per order/item, then numbered) */
        INSERT INTO #src (CustomersOrderReference, OrderingCustomerAccountNumber, ItemNumber, OrderedSalesQuantity, LineAmount, LineNumber)
        SELECT
            d.CustomersOrderReference,
            d.OrderingCustomerAccountNumber,
            d.ItemNumber,
            d.OrderedSalesQuantity,
            d.LineAmount,
            ROW_NUMBER() OVER (PARTITION BY d.CustomersOrderReference ORDER BY d.ItemNumber) AS LineNumber
        FROM
        (
            SELECT
                CAST(h.Docket_Reference AS varchar(50)) AS CustomersOrderReference,
                CAST(h.Account AS varchar(50))          AS OrderingCustomerAccountNumber,
                CAST(l.Dynamics_Code AS varchar(50))    AS ItemNumber,
                SUM(CAST(l.Units_Sold AS decimal(18,4))) AS OrderedSalesQuantity,
                SUM(CAST(COALESCE(l.Line_Net_Amount, l.Sales_Value_Inc_VAT) AS decimal(18,4))) AS LineAmount
            FROM CTL.EPOS_Header h
            JOIN CTL.EPOS_Line_Item l
              ON l.EPOS_Fusion_Key = h.EPOS_Fusion_Key
            WHERE h.Integration_Status = 'READY'
              AND l.Integration_Status = 'READY'
              AND h.[Year] = @ScopeYear
              AND h.[Week] = @ScopeWeek
              AND (@ScopeFile IS NULL OR h.Source_File_ID = @ScopeFile)
              AND l.Dynamics_Code IS NOT NULL
              AND LTRIM(RTRIM(l.Dynamics_Code)) <> ''
            GROUP BY h.Docket_Reference, h.Account, l.Dynamics_Code
        ) d;

        SET @RowsSource = @@ROWCOUNT;

        /* Healing: force any already-STAGED INT rows for this scope to jbro/EA */
        UPDATE i
            SET i.dataAreaId      = @DataAreaId,
                i.SalesUnitSymbol = @SalesUnitSymbol
        FROM INT.SalesOrderStage i
        JOIN #src s
          ON s.CustomersOrderReference = i.CustomersOrderReference
         AND s.ItemNumber              = i.ItemNumber
        WHERE i.Integration_Status = 'STAGED';

        SET @RowsHealed = @@ROWCOUNT;

        /* Insert into INT (avoid duplicates by CustomersOrderReference + ItemNumber) */
        INSERT INTO INT.SalesOrderStage
        (
            dataAreaId,
            CustomersOrderReference,
            CustomerRequisitionNumber,
            OrderingCustomerAccountNumber,
            RequestedShippingDate,
            ItemNumber,
            OrderedSalesQuantity,
            SalesUnitSymbol,
            LineNumber,
            LineAmount,
            LineRequestedShippingDate,
            Integration_Status,
            Created_UTC,
            D365_SONumber
        )
        SELECT
            @DataAreaId,
            s.CustomersOrderReference,
            s.CustomersOrderReference,
            s.OrderingCustomerAccountNumber,
            @RequestedShippingDate,
            s.ItemNumber,
            s.OrderedSalesQuantity,
            @SalesUnitSymbol,
            s.LineNumber,
            s.LineAmount,
            @RequestedShippingDate,
            'STAGED',
            SYSUTCDATETIME(),
            NULL
        FROM #src s
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM INT.SalesOrderStage i
            WHERE i.CustomersOrderReference = s.CustomersOrderReference
              AND i.ItemNumber = s.ItemNumber
        );

        SET @RowsInserted = @@ROWCOUNT;

        /* Update CTL lines READY -> SO_STAGED when corresponding INT row exists (inserted OR already present) */
        UPDATE l
        SET l.Integration_Status = 'SO_STAGED'
        FROM CTL.EPOS_Line_Item l
        JOIN CTL.EPOS_Header h
          ON h.EPOS_Fusion_Key = l.EPOS_Fusion_Key
        JOIN INT.SalesOrderStage i
          ON i.CustomersOrderReference = CAST(h.Docket_Reference AS varchar(50))
         AND i.ItemNumber = CAST(l.Dynamics_Code AS varchar(50))
        WHERE h.[Year] = @ScopeYear
          AND h.[Week] = @ScopeWeek
          AND (@ScopeFile IS NULL OR h.Source_File_ID = @ScopeFile)
          AND h.Integration_Status IN ('READY', 'SO_STAGED')  -- allow healing reruns
          AND l.Integration_Status = 'READY'
          AND i.Integration_Status = 'STAGED'
          AND i.dataAreaId = @DataAreaId
          AND i.SalesUnitSymbol = @SalesUnitSymbol;

        SET @RowsUpdLines = @@ROWCOUNT;

        /* Update CTL headers READY -> SO_STAGED only when NO READY lines remain */
        UPDATE h
        SET h.Integration_Status = 'SO_STAGED'
        FROM CTL.EPOS_Header h
        WHERE h.[Year] = @ScopeYear
          AND h.[Week] = @ScopeWeek
          AND (@ScopeFile IS NULL OR h.Source_File_ID = @ScopeFile)
          AND h.Integration_Status = 'READY'
          AND EXISTS
          (
              SELECT 1
              FROM INT.SalesOrderStage i
              WHERE i.CustomersOrderReference = CAST(h.Docket_Reference AS varchar(50))
                AND i.Integration_Status = 'STAGED'
                AND i.dataAreaId = @DataAreaId
                AND i.SalesUnitSymbol = @SalesUnitSymbol
          )
          AND NOT EXISTS
          (
              SELECT 1
              FROM CTL.EPOS_Line_Item l
              WHERE l.EPOS_Fusion_Key = h.EPOS_Fusion_Key
                AND l.Integration_Status = 'READY'
          );

        SET @RowsUpdHdrs = @@ROWCOUNT;

        COMMIT;

        DECLARE @TxnEnd datetime2(3) = SYSUTCDATETIME();

        /* -----------------------------
           Logs (post-commit)
        ----------------------------- */
        -- INS_INT
        SET @StartUTC = @TxnStart;
        SET @EndUTC   = @TxnEnd;
        SET @Idem     = CONCAT(N'INS_INT;Y=',@ScopeYear,N';W=',@ScopeWeek,N';F=',COALESCE(CONVERT(varchar(20),@ScopeFile),N'*'));
        SET @Msg      = CONCAT(
                        N'INT.SalesOrderStage inserted rows: ', @RowsInserted,
                        N' (source aggregated rows: ', @RowsSource,
                        N', healed staged rows: ', @RowsHealed,
                        N')'
                      );

        INSERT INTO EXC.Process_Transaction_Log
        (
            Run_ID, Txn_ID, Correlation_ID,
            System_Name, Environment,
            Process_Name, SubProcess_Name,
            Step_Name, Step_Code, Step_Category,
            Severity_Level, Execution_Status,
            Rows_Read, Rows_Affected,
            Source_Reference, Reference_Number, Idempotency_Key,
            Message, Error_Message, Details_JSON,
            Start_UTC, End_UTC
        )
        VALUES
        (
            @Run_ID, @Txn_ID, @Correlation_ID,
            @System_Name, @Environment,
            @Process_Name, @SubProcess_Name,
            N'Insert INT SalesOrderStage (STAGED)', N'INS_INT_SO', N'INT_BUILD',
            N'INFO', N'SUCCESS',
            @RowsSource, @RowsInserted,
            N'CTL->INT', NULL, @Idem,
            @Msg, NULL, @Details,
            @StartUTC, @EndUTC
        );

        -- UPD CTL LINES
        SET @Idem = CONCAT(N'UPD_CTL_LNS;Y=',@ScopeYear,N';W=',@ScopeWeek,N';F=',COALESCE(CONVERT(varchar(20),@ScopeFile),N'*'));
        SET @Msg  = CONCAT(N'CTL.EPOS_Line_Item READY->SO_STAGED rows: ', @RowsUpdLines);

        INSERT INTO EXC.Process_Transaction_Log
        (
            Run_ID, Txn_ID, Correlation_ID,
            System_Name, Environment,
            Process_Name, SubProcess_Name,
            Step_Name, Step_Code, Step_Category,
            Severity_Level, Execution_Status,
            Rows_Read, Rows_Affected,
            Source_Reference, Reference_Number, Idempotency_Key,
            Message, Error_Message, Details_JSON,
            Start_UTC, End_UTC
        )
        VALUES
        (
            @Run_ID, @Txn_ID, @Correlation_ID,
            @System_Name, @Environment,
            @Process_Name, @SubProcess_Name,
            N'Update CTL line statuses', N'UPD_CTL_LNS', N'CTL_UPDATE',
            N'INFO', N'SUCCESS',
            @RowsUpdLines, @RowsUpdLines,
            N'CTL.EPOS_Line_Item', NULL, @Idem,
            @Msg, NULL, @Details,
            @StartUTC, @EndUTC
        );

        -- UPD CTL HEADERS
        SET @Idem = CONCAT(N'UPD_CTL_HDR;Y=',@ScopeYear,N';W=',@ScopeWeek,N';F=',COALESCE(CONVERT(varchar(20),@ScopeFile),N'*'));
        SET @Msg  = CONCAT(N'CTL.EPOS_Header READY->SO_STAGED rows: ', @RowsUpdHdrs);

        INSERT INTO EXC.Process_Transaction_Log
        (
            Run_ID, Txn_ID, Correlation_ID,
            System_Name, Environment,
            Process_Name, SubProcess_Name,
            Step_Name, Step_Code, Step_Category,
            Severity_Level, Execution_Status,
            Rows_Read, Rows_Affected,
            Source_Reference, Reference_Number, Idempotency_Key,
            Message, Error_Message, Details_JSON,
            Start_UTC, End_UTC
        )
        VALUES
        (
            @Run_ID, @Txn_ID, @Correlation_ID,
            @System_Name, @Environment,
            @Process_Name, @SubProcess_Name,
            N'Update CTL header statuses', N'UPD_CTL_HDR', N'CTL_UPDATE',
            N'INFO', N'SUCCESS',
            @RowsUpdHdrs, @RowsUpdHdrs,
            N'CTL.EPOS_Header', NULL, @Idem,
            @Msg, NULL, @Details,
            @StartUTC, @EndUTC
        );

        -- STEP6_END
        SET @StartUTC = SYSUTCDATETIME();
        SET @EndUTC   = @StartUTC;
        SET @Idem     = CONCAT(N'S6_END;Y=',@ScopeYear,N';W=',@ScopeWeek,N';F=',COALESCE(CONVERT(varchar(20),@ScopeFile),N'*'));
        SET @Msg      = CONCAT(N'Step 6 complete. Inserted=',@RowsInserted,N', Healed=',@RowsHealed,N', LinesUpdated=',@RowsUpdLines,N', HeadersUpdated=',@RowsUpdHdrs);

        INSERT INTO EXC.Process_Transaction_Log
        (
            Run_ID, Txn_ID, Correlation_ID,
            System_Name, Environment,
            Process_Name, SubProcess_Name,
            Step_Name, Step_Code, Step_Category,
            Severity_Level, Execution_Status,
            Rows_Read, Rows_Affected,
            Source_Reference, Reference_Number, Idempotency_Key,
            Message, Error_Message, Details_JSON,
            Start_UTC, End_UTC
        )
        VALUES
        (
            @Run_ID, @Txn_ID, @Correlation_ID,
            @System_Name, @Environment,
            @Process_Name, @SubProcess_Name,
            N'End Step 6', N'STEP6_END', N'RUN_CONTROL',
            N'INFO', N'SUCCESS',
            0, 0,
            N'ALL', NULL, @Idem,
            @Msg, NULL, @Details,
            @StartUTC, @EndUTC
        );

        /* Return summary */
        SELECT
              @Run_ID               AS Run_ID
            , @Txn_ID               AS Txn_ID
            , @Correlation_ID       AS Correlation_ID
            , @ScopeYear            AS [Year]
            , @ScopeWeek            AS [Week]
            , @ScopeFile            AS File_ID
            , @RowsSource           AS Source_Lines
            , @RowsInserted         AS INT_RowsInserted
            , @RowsHealed           AS INT_RowsHealed
            , @RowsUpdLines         AS CTL_LinesUpdated
            , @RowsUpdHdrs          AS CTL_HeadersUpdated
            , @RequestedShippingDate AS RequestedShippingDate
            , @DataAreaId           AS DataAreaId_Enforced
            , @SalesUnitSymbol      AS SalesUnitSymbol_Enforced;

        RETURN 0;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE @Err nvarchar(4000) = ERROR_MESSAGE();
        DECLARE @ErrUTC datetime2(3) = SYSUTCDATETIME();

        SET @Idem = CONCAT(N'S6_FAIL;Y=',COALESCE(CONVERT(varchar(20),@ScopeYear),N'*'),
                           N';W=',COALESCE(CONVERT(varchar(20),@ScopeWeek),N'*'),
                           N';F=',COALESCE(CONVERT(varchar(20),@ScopeFile),N'*'));

        INSERT INTO EXC.Process_Transaction_Log
        (
            Run_ID, Txn_ID, Correlation_ID,
            System_Name, Environment,
            Process_Name, SubProcess_Name,
            Step_Name, Step_Code, Step_Category,
            Severity_Level, Execution_Status,
            Rows_Read, Rows_Affected,
            Source_Reference, Reference_Number, Idempotency_Key,
            Message, Error_Message, Details_JSON,
            Start_UTC, End_UTC
        )
        VALUES
        (
            @Run_ID, @Txn_ID, @Correlation_ID,
            @System_Name, @Environment,
            @Process_Name, @SubProcess_Name,
            N'Step 6 failed', N'STEP6_FAIL', N'RUN_CONTROL',
            N'ERROR', N'FAILED',
            NULL, NULL,
            N'ALL', NULL, @Idem,
            N'Error staging weekly orders to INT.SalesOrderStage', @Err, @Details,
            @ErrUTC, @ErrUTC
        );

        ;THROW;
    END CATCH
END;
GO
