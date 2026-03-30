-- =============================================================
--  Procedure : [EXC].[usp_EPOS_Generate_Weekly_Orders_Step4]
--  Database  : Fusion_EPOS_Production
--  Exported  : 2026-03-30 02:45:06
--  Created   : 2026-02-12 14:23:52.953000
--  Modified  : 2026-02-12 15:15:08.840000
-- =============================================================
USE [Fusion_EPOS_Production]
GO

/* ============================================================
   STEP 4 - EPOS_Generate_Weekly_Orders
   Single supporting SP in EXC schema (master is Python)
   - RAW scope: ISNULL(SynProcessed,0)=0
   - Store join: INT-normalised (TRY_CAST(Store AS int) = TRY_CAST(StoreCode AS int))
   - Inserts CTL Header + Lines as STAGED
   - Logs + marks REMOVED for Units_Sold <= 0
   - Aggregates duplicate Dynamics_Code within EPOS_Fusion_Key (temp table approach)
       * logs duplicates as Aggregated into EXC.Processed_Lines
       * adds duplicate qty/value onto keeper line
       * marks duplicate lines REMOVED
   - Promotes remaining eligible lines to READY
   - Promotes headers to READY or NO_LINES
   - Atomic step telemetry into EXC.Process_Transaction_Log
   ============================================================ */

CREATE   PROCEDURE [EXC].[usp_EPOS_Generate_Weekly_Orders_Step4]
(
      @Environment       varchar(50) = 'PROD'
    , @System_Name       varchar(50) = 'Fusion_EPOS'

    , @Run_ID            bigint = NULL
    , @Txn_ID            bigint = NULL
    , @Correlation_ID    uniqueidentifier = NULL

    -- Optional scope filters (still only SynProcessed=0 rows are eligible)
    , @File_ID           int = NULL
    , @Year              int = NULL
    , @Week              int = NULL

    , @MarkRawProcessed  bit = 1
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE 
          @Process_Name     varchar(200) = 'EPOS_Generate_Weekly_Orders'
        , @SubProcess_Name  varchar(200) = 'Step4';

    IF @Run_ID IS NULL
        SET @Run_ID = CONVERT(bigint, DATEDIFF_BIG(MILLISECOND, '1970-01-01', SYSUTCDATETIME()));
    IF @Txn_ID IS NULL
        SET @Txn_ID = @Run_ID + 10;
    IF @Correlation_ID IS NULL
        SET @Correlation_ID = NEWID();

    DECLARE
          @Log_ID          bigint
        , @StepStartUTC    datetime2(7)
        , @RowsAffected    bigint;

    /* =========================================================
       STEP 4.01 - START
       ========================================================= */
    SET @StepStartUTC = SYSUTCDATETIME();
    INSERT INTO EXC.Process_Transaction_Log
    (
        Run_ID, Txn_ID, Correlation_ID, System_Name, Environment,
        Process_Name, SubProcess_Name,
        Step_Name, Step_Code, Step_Category,
        Severity_Level, Execution_Status,
        Source_Reference, Idempotency_Key,
        Message, Start_UTC
    )
    VALUES
    (
        @Run_ID, @Txn_ID, @Correlation_ID, @System_Name, @Environment,
        @Process_Name, @SubProcess_Name,
        'Start Step 4', 'STEP4_START', 'RUN_CONTROL',
        'INFO', 'RUNNING',
        COALESCE(CAST(@File_ID AS varchar(50)),'ALL'),
        CONCAT('SynProcessed=0;Y=',COALESCE(CAST(@Year AS varchar(20)),'*'),
               ';W=',COALESCE(CAST(@Week AS varchar(20)),'*'),
               ';F=',COALESCE(CAST(@File_ID AS varchar(20)),'*')),
        'Starting Step 4', @StepStartUTC
    );
    SET @Log_ID = CONVERT(bigint, SCOPE_IDENTITY());

    UPDATE EXC.Process_Transaction_Log
    SET Execution_Status = 'SUCCESS',
        Rows_Read = 0,
        Rows_Affected = 0,
        End_UTC = SYSUTCDATETIME(),
        Message = 'Step 4 started'
    WHERE Log_ID = @Log_ID;

    /* =========================================================
       STEP 4.02 - WARN: STORE PARSE FAILURES (Store not castable to INT)
       ========================================================= */
    BEGIN TRY
        SET @StepStartUTC = SYSUTCDATETIME();
        INSERT INTO EXC.Process_Transaction_Log
        (
            Run_ID, Txn_ID, Correlation_ID, System_Name, Environment,
            Process_Name, SubProcess_Name,
            Step_Name, Step_Code, Step_Category,
            Severity_Level, Execution_Status,
            Source_Reference, Idempotency_Key,
            Message, Start_UTC
        )
        VALUES
        (
            @Run_ID, @Txn_ID, @Correlation_ID, @System_Name, @Environment,
            @Process_Name, @SubProcess_Name,
            'Check store parse failures', 'CHK_STORE_PARSE', 'RUN_CONTROL',
            'INFO', 'RUNNING',
            COALESCE(CAST(@File_ID AS varchar(50)),'ALL'),
            CONCAT('STORE_PARSE;F=',COALESCE(CAST(@File_ID AS varchar(20)),'*')),
            'Detecting RAW.Store values not castable to INT', @StepStartUTC
        );
        SET @Log_ID = CONVERT(bigint, SCOPE_IDENTITY());

        DECLARE @StoreParseFailCount bigint;

        ;WITH raw_scope AS
        (
            SELECT DISTINCT r.Store
            FROM RAW.EPOS_LineItems r
            WHERE ISNULL(r.SynProcessed,0) = 0
              AND (@File_ID IS NULL OR r.File_ID = @File_ID)
              AND (@Year   IS NULL OR r.[Year] = @Year)
              AND (@Week   IS NULL OR r.[Week] = @Week)
        )
        SELECT @StoreParseFailCount = COUNT(*)
        FROM raw_scope
        WHERE TRY_CAST(Store AS int) IS NULL;

        UPDATE EXC.Process_Transaction_Log
        SET Execution_Status = 'SUCCESS',
            Severity_Level = CASE WHEN @StoreParseFailCount > 0 THEN 'WARN' ELSE 'INFO' END,
            Rows_Affected = @StoreParseFailCount,
            End_UTC = SYSUTCDATETIME(),
            Message = CASE
                        WHEN @StoreParseFailCount > 0 THEN CONCAT('WARNING: ', @StoreParseFailCount, ' RAW.Store values cannot be cast to INT. These will be SKIPPED.')
                        ELSE 'All RAW.Store values cast to INT successfully'
                      END
        WHERE Log_ID = @Log_ID;
    END TRY
    BEGIN CATCH
        UPDATE EXC.Process_Transaction_Log
        SET Execution_Status = 'FAILED',
            Severity_Level = 'ERROR',
            Error_Message = ERROR_MESSAGE(),
            End_UTC = SYSUTCDATETIME()
        WHERE Log_ID = @Log_ID;
        THROW;
    END CATCH;

    /* =========================================================
       STEP 4.03 - WARN: MISSING STORE MAPPINGS (after INT normalisation)
       ========================================================= */
    BEGIN TRY
        SET @StepStartUTC = SYSUTCDATETIME();
        INSERT INTO EXC.Process_Transaction_Log
        (
            Run_ID, Txn_ID, Correlation_ID, System_Name, Environment,
            Process_Name, SubProcess_Name,
            Step_Name, Step_Code, Step_Category,
            Severity_Level, Execution_Status,
            Source_Reference, Idempotency_Key,
            Message, Start_UTC
        )
        VALUES
        (
            @Run_ID, @Txn_ID, @Correlation_ID, @System_Name, @Environment,
            @Process_Name, @SubProcess_Name,
            'Check missing store mappings', 'CHK_STORE_MAP', 'RUN_CONTROL',
            'INFO', 'RUNNING',
            COALESCE(CAST(@File_ID AS varchar(50)),'ALL'),
            CONCAT('STOREMAP_INT;F=',COALESCE(CAST(@File_ID AS varchar(20)),'*')),
            'Detecting RAW stores with no CFG mapping (INT join)', @StepStartUTC
        );
        SET @Log_ID = CONVERT(bigint, SCOPE_IDENTITY());

        DECLARE @MissingStoreMapCount bigint;

        ;WITH raw_scope AS
        (
            SELECT DISTINCT TRY_CAST(r.Store AS int) AS StoreInt
            FROM RAW.EPOS_LineItems r
            WHERE ISNULL(r.SynProcessed,0) = 0
              AND (@File_ID IS NULL OR r.File_ID = @File_ID)
              AND (@Year   IS NULL OR r.[Year] = @Year)
              AND (@Week   IS NULL OR r.[Week] = @Week)
              AND TRY_CAST(r.Store AS int) IS NOT NULL
        )
        SELECT @MissingStoreMapCount = COUNT(*)
        FROM raw_scope rs
        LEFT JOIN CFG.Dynamics_Stores ds
          ON TRY_CAST(ds.StoreCode AS int) = rs.StoreInt
        WHERE ds.StoreCode IS NULL;

        UPDATE EXC.Process_Transaction_Log
        SET Execution_Status = 'SUCCESS',
            Severity_Level = CASE WHEN @MissingStoreMapCount > 0 THEN 'WARN' ELSE 'INFO' END,
            Rows_Affected = @MissingStoreMapCount,
            End_UTC = SYSUTCDATETIME(),
            Message = CASE
                        WHEN @MissingStoreMapCount > 0 THEN CONCAT('WARNING: Missing store mappings (INT join): ', @MissingStoreMapCount, '. Orders will be SKIPPED (RAW remains unprocessed).')
                        ELSE 'All RAW stores have CFG mappings (INT join)'
                      END
        WHERE Log_ID = @Log_ID;
    END TRY
    BEGIN CATCH
        UPDATE EXC.Process_Transaction_Log
        SET Execution_Status = 'FAILED',
            Severity_Level = 'ERROR',
            Error_Message = ERROR_MESSAGE(),
            End_UTC = SYSUTCDATETIME()
        WHERE Log_ID = @Log_ID;
        THROW;
    END CATCH;

    /* =========================================================
       STEP 4.04 - INSERT CTL HEADERS (STAGED) from ELIGIBLE scope
       Eligible = SynProcessed=0 AND Store casts to INT AND maps to CFG
       ========================================================= */
    BEGIN TRY
        SET @StepStartUTC = SYSUTCDATETIME();
        INSERT INTO EXC.Process_Transaction_Log
        (
            Run_ID, Txn_ID, Correlation_ID, System_Name, Environment,
            Process_Name, SubProcess_Name,
            Step_Name, Step_Code, Step_Category,
            Severity_Level, Execution_Status,
            Source_Reference, Idempotency_Key,
            Message, Start_UTC
        )
        VALUES
        (
            @Run_ID, @Txn_ID, @Correlation_ID, @System_Name, @Environment,
            @Process_Name, @SubProcess_Name,
            'Insert CTL headers (STAGED)', 'INS_CTL_HDR', 'CTL_BUILD',
            'INFO', 'RUNNING',
            COALESCE(CAST(@File_ID AS varchar(50)),'ALL'),
            CONCAT('HDR;SynProcessed=0;INTJoin;F=',COALESCE(CAST(@File_ID AS varchar(20)),'*')),
            'Insert headers for eligible RAW rows (INT-normalised store join)', @StepStartUTC
        );
        SET @Log_ID = CONVERT(bigint, SCOPE_IDENTITY());

        ;WITH raw_scope AS
        (
            SELECT *
            FROM RAW.EPOS_LineItems r
            WHERE ISNULL(r.SynProcessed,0) = 0
              AND (@File_ID IS NULL OR r.File_ID = @File_ID)
              AND (@Year   IS NULL OR r.[Year] = @Year)
              AND (@Week   IS NULL OR r.[Week] = @Week)
              AND TRY_CAST(r.Store AS int) IS NOT NULL
        ),
        eligible AS
        (
            SELECT r.*
            FROM raw_scope r
            JOIN CFG.Dynamics_Stores ds
              ON TRY_CAST(ds.StoreCode AS int) = TRY_CAST(r.Store AS int)
        ),
        src AS
        (
            SELECT
                  r.File_ID
                , r.Store
                , r.Docket_Reference
                , MAX(r.Region) AS Region
                , MAX(r.[Year]) AS [Year]
                , MAX(r.[Week]) AS [Week]
                , SUM(r.Units_Sold) AS EPOS_Total_Units
                , SUM(r.Sales_Value_Inc_VAT) AS EPOS_Total_Sales
                , COUNT(*) AS EPOS_Line_Count
            FROM eligible r
            GROUP BY r.File_ID, r.Store, r.Docket_Reference
        )
        INSERT INTO CTL.EPOS_Header
        (
              EPOS_Fusion_Key
            , Fusion_Code
            , Docket_Reference
            , Store
            , Store_Name
            , Region
            , [Year]
            , [Week]
            , EPOS_Total_Units
            , EPOS_Total_Sales
            , EPOS_Line_Count
            , Source_File_ID
            , Store_Code
            , Account
            , Warehouse
            , Primeline_Edi_Ana
            , Cust_Edi_Ana
            , Del_Edi_Ana
            , Sales_Order_Pool
            , Created_UTC
            , Integration_Status
            , D365_SO_Number
            , D365_Invoice_Number
        )
        SELECT
              CONCAT('FEPOS-', CAST(s.Docket_Reference AS varchar(50))) AS EPOS_Fusion_Key
            , CONCAT('FEPOS-', CAST(s.Docket_Reference AS varchar(50))) AS Fusion_Code
            , CAST(s.Docket_Reference AS varchar(50)) AS Docket_Reference
            , s.Store
            , MAX(ds.Name) AS Store_Name
            , s.Region
            , s.[Year]
            , s.[Week]
            , s.EPOS_Total_Units
            , s.EPOS_Total_Sales
            , s.EPOS_Line_Count
            , s.File_ID
            , MAX(ds.StoreCode) AS Store_Code
            , MAX(ds.Account) AS Account
            , MAX(ds.Warehouse) AS Warehouse
            , MAX(ds.Primeline_Edi_Ana) AS Primeline_Edi_Ana
            , MAX(ds.Cust_Edi_ana) AS Cust_Edi_Ana
            , MAX(ds.Del_Edi_Ana) AS Del_Edi_Ana
            , MAX(ds.Sales_order_pool) AS Sales_Order_Pool
            , SYSUTCDATETIME()
            , 'STAGED'
            , NULL
            , NULL
        FROM src s
        JOIN CFG.Dynamics_Stores ds
          ON TRY_CAST(ds.StoreCode AS int) = TRY_CAST(s.Store AS int)
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM CTL.EPOS_Header h
            WHERE h.Source_File_ID = s.File_ID
              AND h.Store = s.Store
              AND h.Docket_Reference = CAST(s.Docket_Reference AS varchar(50))
        )
        GROUP BY
              s.File_ID
            , s.Store
            , s.Docket_Reference
            , s.Region
            , s.[Year]
            , s.[Week]
            , s.EPOS_Total_Units
            , s.EPOS_Total_Sales
            , s.EPOS_Line_Count;

        SET @RowsAffected = @@ROWCOUNT;

        UPDATE EXC.Process_Transaction_Log
        SET Execution_Status = 'SUCCESS',
            Rows_Affected = @RowsAffected,
            End_UTC = SYSUTCDATETIME(),
            Message = CONCAT('Inserted CTL headers: ', @RowsAffected)
        WHERE Log_ID = @Log_ID;
    END TRY
    BEGIN CATCH
        UPDATE EXC.Process_Transaction_Log
        SET Execution_Status = 'FAILED',
            Severity_Level = 'ERROR',
            Error_Message = ERROR_MESSAGE(),
            End_UTC = SYSUTCDATETIME()
        WHERE Log_ID = @Log_ID;
        THROW;
    END CATCH;

    /* =========================================================
       STEP 4.05 - INSERT CTL LINES (STAGED) from ELIGIBLE scope
       Eligible = SynProcessed=0 AND header exists (prevents FK)
       ========================================================= */
    BEGIN TRY
        SET @StepStartUTC = SYSUTCDATETIME();
        INSERT INTO EXC.Process_Transaction_Log
        (
            Run_ID, Txn_ID, Correlation_ID, System_Name, Environment,
            Process_Name, SubProcess_Name,
            Step_Name, Step_Code, Step_Category,
            Severity_Level, Execution_Status,
            Source_Reference, Idempotency_Key,
            Message, Start_UTC
        )
        VALUES
        (
            @Run_ID, @Txn_ID, @Correlation_ID, @System_Name, @Environment,
            @Process_Name, @SubProcess_Name,
            'Insert CTL lines (STAGED)', 'INS_CTL_LNS', 'CTL_BUILD',
            'INFO', 'RUNNING',
            COALESCE(CAST(@File_ID AS varchar(50)),'ALL'),
            CONCAT('LNS;SynProcessed=0;HeaderExists;INTJoin;F=',COALESCE(CAST(@File_ID AS varchar(20)),'*')),
            'Insert lines only where CTL header exists', @StepStartUTC
        );
        SET @Log_ID = CONVERT(bigint, SCOPE_IDENTITY());

        ;WITH raw_scope AS
        (
            SELECT *
            FROM RAW.EPOS_LineItems r
            WHERE ISNULL(r.SynProcessed,0) = 0
              AND (@File_ID IS NULL OR r.File_ID = @File_ID)
              AND (@Year   IS NULL OR r.[Year] = @Year)
              AND (@Week   IS NULL OR r.[Week] = @Week)
        ),
        eligible AS
        (
            SELECT r.*
            FROM raw_scope r
            WHERE EXISTS
            (
                SELECT 1
                FROM CTL.EPOS_Header h
                WHERE h.Source_File_ID = r.File_ID
                  AND h.Store = r.Store
                  AND h.Docket_Reference = CAST(r.Docket_Reference AS varchar(50))
            )
        )
        INSERT INTO CTL.EPOS_Line_Item
        (
              EPOS_Fusion_Key
            , Source_LineItem_ID
            , Source_File_ID
            , Retail_Barcode
            , Dunnes_Prod_Code
            , Product_Description
            , Units_Sold
            , Sales_Value_Inc_VAT
            , Dynamics_Code
            , Created_UTC
            , Integration_Status
            , Invoice_Financial_Qty
            , Qty_Retail_Units
            , Line_Net_Amount
            , Unit_Cost
            , Net_Unit_Cost
        )
        SELECT
              CONCAT('FEPOS-', CAST(r.Docket_Reference AS varchar(50))) AS EPOS_Fusion_Key
            , r.LineItem_ID
            , r.File_ID
            , r.Retail_Barcode
            , r.Dunnes_Prod_Code
            , r.Product_Description
            , r.Units_Sold
            , r.Sales_Value_Inc_VAT
            , p.Dynamics_Code
            , SYSUTCDATETIME()
            , 'STAGED'
            , NULL, NULL, NULL, NULL, NULL
        FROM eligible r
        LEFT JOIN CFG.Products p
          ON r.Dunnes_Prod_Code = p.Dunnes_Prod_Code
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM CTL.EPOS_Line_Item l
            WHERE l.Source_File_ID = r.File_ID
              AND l.Source_LineItem_ID = r.LineItem_ID
        );

        SET @RowsAffected = @@ROWCOUNT;

        /* Mark RAW processed ONLY for rows whose header exists (eligible set) */
        IF @MarkRawProcessed = 1
        BEGIN
            UPDATE r
            SET r.SynProcessed = 1
            FROM RAW.EPOS_LineItems r
            WHERE ISNULL(r.SynProcessed,0) = 0
              AND (@File_ID IS NULL OR r.File_ID = @File_ID)
              AND (@Year   IS NULL OR r.[Year] = @Year)
              AND (@Week   IS NULL OR r.[Week] = @Week)
              AND EXISTS
              (
                  SELECT 1
                  FROM CTL.EPOS_Header h
                  WHERE h.Source_File_ID = r.File_ID
                    AND h.Store = r.Store
                    AND h.Docket_Reference = CAST(r.Docket_Reference AS varchar(50))
              );
        END

        UPDATE EXC.Process_Transaction_Log
        SET Execution_Status = 'SUCCESS',
            Rows_Affected = @RowsAffected,
            End_UTC = SYSUTCDATETIME(),
            Message = CONCAT('Inserted CTL lines: ', @RowsAffected, '; RAW SynProcessed set to 1 for eligible rows (if enabled)')
        WHERE Log_ID = @Log_ID;
    END TRY
    BEGIN CATCH
        UPDATE EXC.Process_Transaction_Log
        SET Execution_Status = 'FAILED',
            Severity_Level = 'ERROR',
            Error_Message = ERROR_MESSAGE(),
            End_UTC = SYSUTCDATETIME()
        WHERE Log_ID = @Log_ID;
        THROW;
    END CATCH;

    /* =========================================================
       STEP 4.06 - LOG + REMOVE INVALID QTY (<=0)
       ========================================================= */
    BEGIN TRY
        SET @StepStartUTC = SYSUTCDATETIME();
        INSERT INTO EXC.Process_Transaction_Log
        (
            Run_ID, Txn_ID, Correlation_ID, System_Name, Environment,
            Process_Name, SubProcess_Name,
            Step_Name, Step_Code, Step_Category,
            Severity_Level, Execution_Status,
            Source_Reference, Idempotency_Key,
            Message, Start_UTC
        )
        VALUES
        (
            @Run_ID, @Txn_ID, @Correlation_ID, @System_Name, @Environment,
            @Process_Name, @SubProcess_Name,
            'Remove invalid qty (<=0)', 'REM_INVALID_QTY', 'CLEANSE',
            'INFO', 'RUNNING',
            COALESCE(CAST(@File_ID AS varchar(50)),'ALL'),
            CONCAT('INVQTY;F=',COALESCE(CAST(@File_ID AS varchar(20)),'*')),
            'Log + mark REMOVED where Units_Sold <= 0', @StepStartUTC
        );
        SET @Log_ID = CONVERT(bigint, SCOPE_IDENTITY());

        INSERT INTO EXC.Processed_Lines
        (
              EPOS_Fusion_Key
            , EPOS_Line_ID
            , CustomersOrderReference
            , ItemNumber
            , Original_Quantity
            , Processed_Action
            , Reason
            , Processed_UTC
        )
        SELECT
              l.EPOS_Fusion_Key
            , l.EPOS_Line_ID
            , h.Docket_Reference
            , l.Dynamics_Code
            , l.Units_Sold
            , 'Removed'
            , CASE WHEN l.Units_Sold < 0 THEN 'Negative quantity' ELSE 'Quantity = 0' END
            , SYSUTCDATETIME()
        FROM CTL.EPOS_Line_Item l
        JOIN CTL.EPOS_Header h
          ON h.EPOS_Fusion_Key = l.EPOS_Fusion_Key
        WHERE
            l.Integration_Status = 'STAGED'
            AND l.Units_Sold <= 0
            AND (@File_ID IS NULL OR l.Source_File_ID = @File_ID)
            AND NOT EXISTS
            (
                SELECT 1
                FROM EXC.Processed_Lines pl
                WHERE pl.EPOS_Line_ID = l.EPOS_Line_ID
                  AND pl.Processed_Action = 'Removed'
            );

        UPDATE l
        SET l.Integration_Status = 'REMOVED'
        FROM CTL.EPOS_Line_Item l
        WHERE
            l.Integration_Status = 'STAGED'
            AND l.Units_Sold <= 0
            AND (@File_ID IS NULL OR l.Source_File_ID = @File_ID);

        SET @RowsAffected = @@ROWCOUNT;

        UPDATE EXC.Process_Transaction_Log
        SET Execution_Status = 'SUCCESS',
            Rows_Affected = @RowsAffected,
            End_UTC = SYSUTCDATETIME(),
            Message = CONCAT('Invalid qty lines removed: ', @RowsAffected)
        WHERE Log_ID = @Log_ID;
    END TRY
    BEGIN CATCH
        UPDATE EXC.Process_Transaction_Log
        SET Execution_Status = 'FAILED',
            Severity_Level = 'ERROR',
            Error_Message = ERROR_MESSAGE(),
            End_UTC = SYSUTCDATETIME()
        WHERE Log_ID = @Log_ID;
        THROW;
    END CATCH;

    /* =========================================================
       STEP 4.07 - AGGREGATE DUPLICATE Dynamics_Code PER ORDER
       Temp-table approach avoids CTE scope issues
       ========================================================= */
    BEGIN TRY
        SET @StepStartUTC = SYSUTCDATETIME();
        INSERT INTO EXC.Process_Transaction_Log
        (
            Run_ID, Txn_ID, Correlation_ID, System_Name, Environment,
            Process_Name, SubProcess_Name,
            Step_Name, Step_Code, Step_Category,
            Severity_Level, Execution_Status,
            Source_Reference, Idempotency_Key,
            Message, Start_UTC
        )
        VALUES
        (
            @Run_ID, @Txn_ID, @Correlation_ID, @System_Name, @Environment,
            @Process_Name, @SubProcess_Name,
            'Aggregate duplicate items', 'AGG_DUP_ITEMS', 'CLEANSE',
            'INFO', 'RUNNING',
            COALESCE(CAST(@File_ID AS varchar(50)),'ALL'),
            CONCAT('AGG;F=',COALESCE(CAST(@File_ID AS varchar(20)),'*')),
            'Aggregate duplicate Dynamics_Code per EPOS_Fusion_Key', @StepStartUTC
        );
        SET @Log_ID = CONVERT(bigint, SCOPE_IDENTITY());

        IF OBJECT_ID('tempdb..#AggCandidates') IS NOT NULL DROP TABLE #AggCandidates;

        SELECT
              l.EPOS_Line_ID
            , l.EPOS_Fusion_Key
            , l.Dynamics_Code
            , l.Units_Sold
            , l.Sales_Value_Inc_VAT
            , ROW_NUMBER() OVER
              (
                PARTITION BY l.EPOS_Fusion_Key, l.Dynamics_Code
                ORDER BY l.EPOS_Line_ID
              ) AS rn
        INTO #AggCandidates
        FROM CTL.EPOS_Line_Item l
        WHERE
            l.Integration_Status = 'STAGED'
            AND l.Units_Sold > 0
            AND l.Dynamics_Code IS NOT NULL
            AND LTRIM(RTRIM(l.Dynamics_Code)) <> ''
            AND (@File_ID IS NULL OR l.Source_File_ID = @File_ID);

        /* Log duplicates as Aggregated */
        INSERT INTO EXC.Processed_Lines
        (
              EPOS_Fusion_Key
            , EPOS_Line_ID
            , CustomersOrderReference
            , ItemNumber
            , Original_Quantity
            , Processed_Action
            , Reason
            , Processed_UTC
        )
        SELECT
              c.EPOS_Fusion_Key
            , c.EPOS_Line_ID
            , h.Docket_Reference
            , c.Dynamics_Code
            , c.Units_Sold
            , 'Aggregated'
            , 'Duplicate product aggregated into single order line'
            , SYSUTCDATETIME()
        FROM #AggCandidates c
        JOIN CTL.EPOS_Header h
          ON h.EPOS_Fusion_Key = c.EPOS_Fusion_Key
        WHERE c.rn > 1
          AND NOT EXISTS
          (
              SELECT 1
              FROM EXC.Processed_Lines pl
              WHERE pl.EPOS_Line_ID = c.EPOS_Line_ID
                AND pl.Processed_Action = 'Aggregated'
          );

        /* Update keeper line by adding duplicate totals */
        ;WITH sums AS
        (
            SELECT
                  EPOS_Fusion_Key
                , Dynamics_Code
                , SUM(Units_Sold) AS AddQty
                , SUM(Sales_Value_Inc_VAT) AS AddSales
            FROM #AggCandidates
            WHERE rn > 1
            GROUP BY EPOS_Fusion_Key, Dynamics_Code
        )
        UPDATE k
        SET
            k.Units_Sold = k.Units_Sold + s.AddQty,
            k.Sales_Value_Inc_VAT = k.Sales_Value_Inc_VAT + s.AddSales
        FROM CTL.EPOS_Line_Item k
        JOIN #AggCandidates c
          ON c.EPOS_Line_ID = k.EPOS_Line_ID
         AND c.rn = 1
        JOIN sums s
          ON s.EPOS_Fusion_Key = c.EPOS_Fusion_Key
         AND s.Dynamics_Code = c.Dynamics_Code;

        /* Mark dupes REMOVED */
        UPDATE l
        SET l.Integration_Status = 'REMOVED'
        FROM CTL.EPOS_Line_Item l
        JOIN #AggCandidates c
          ON c.EPOS_Line_ID = l.EPOS_Line_ID
        WHERE c.rn > 1;

        SET @RowsAffected = @@ROWCOUNT;

        DROP TABLE #AggCandidates;

        UPDATE EXC.Process_Transaction_Log
        SET Execution_Status = 'SUCCESS',
            Rows_Affected = @RowsAffected,
            End_UTC = SYSUTCDATETIME(),
            Message = CONCAT('Duplicate lines aggregated+removed: ', @RowsAffected)
        WHERE Log_ID = @Log_ID;
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#AggCandidates') IS NOT NULL DROP TABLE #AggCandidates;

        UPDATE EXC.Process_Transaction_Log
        SET Execution_Status = 'FAILED',
            Severity_Level = 'ERROR',
            Error_Message = ERROR_MESSAGE(),
            End_UTC = SYSUTCDATETIME()
        WHERE Log_ID = @Log_ID;
        THROW;
    END CATCH;

    /* =========================================================
       STEP 4.08 - LINES: STAGED -> READY (eligible only)
       ========================================================= */
    BEGIN TRY
        SET @StepStartUTC = SYSUTCDATETIME();
        INSERT INTO EXC.Process_Transaction_Log
        (
            Run_ID, Txn_ID, Correlation_ID, System_Name, Environment,
            Process_Name, SubProcess_Name,
            Step_Name, Step_Code, Step_Category,
            Severity_Level, Execution_Status,
            Source_Reference, Idempotency_Key,
            Message, Start_UTC
        )
        VALUES
        (
            @Run_ID, @Txn_ID, @Correlation_ID, @System_Name, @Environment,
            @Process_Name, @SubProcess_Name,
            'Promote lines to READY', 'UPD_LNS_READY', 'FINALISE',
            'INFO', 'RUNNING',
            COALESCE(CAST(@File_ID AS varchar(50)),'ALL'),
            CONCAT('READY;F=',COALESCE(CAST(@File_ID AS varchar(20)),'*')),
            'Set eligible STAGED lines to READY', @StepStartUTC
        );
        SET @Log_ID = CONVERT(bigint, SCOPE_IDENTITY());

        UPDATE l
        SET l.Integration_Status = 'READY'
        FROM CTL.EPOS_Line_Item l
        WHERE
            l.Integration_Status = 'STAGED'
            AND l.Units_Sold > 0
            AND l.Dynamics_Code IS NOT NULL
            AND LTRIM(RTRIM(l.Dynamics_Code)) <> ''
            AND (@File_ID IS NULL OR l.Source_File_ID = @File_ID);

        SET @RowsAffected = @@ROWCOUNT;

        UPDATE EXC.Process_Transaction_Log
        SET Execution_Status = 'SUCCESS',
            Rows_Affected = @RowsAffected,
            End_UTC = SYSUTCDATETIME(),
            Message = CONCAT('Lines set to READY: ', @RowsAffected)
        WHERE Log_ID = @Log_ID;
    END TRY
    BEGIN CATCH
        UPDATE EXC.Process_Transaction_Log
        SET Execution_Status = 'FAILED',
            Severity_Level = 'ERROR',
            Error_Message = ERROR_MESSAGE(),
            End_UTC = SYSUTCDATETIME()
        WHERE Log_ID = @Log_ID;
        THROW;
    END CATCH;

    /* =========================================================
       STEP 4.09 - HEADERS: STAGED -> READY / NO_LINES
       ========================================================= */
    BEGIN TRY
        SET @StepStartUTC = SYSUTCDATETIME();
        INSERT INTO EXC.Process_Transaction_Log
        (
            Run_ID, Txn_ID, Correlation_ID, System_Name, Environment,
            Process_Name, SubProcess_Name,
            Step_Name, Step_Code, Step_Category,
            Severity_Level, Execution_Status,
            Source_Reference, Idempotency_Key,
            Message, Start_UTC
        )
        VALUES
        (
            @Run_ID, @Txn_ID, @Correlation_ID, @System_Name, @Environment,
            @Process_Name, @SubProcess_Name,
            'Promote headers READY/NO_LINES', 'UPD_HDR_STATUS', 'FINALISE',
            'INFO', 'RUNNING',
            COALESCE(CAST(@File_ID AS varchar(50)),'ALL'),
            CONCAT('HDR;F=',COALESCE(CAST(@File_ID AS varchar(20)),'*')),
            'Set headers READY if any READY lines else NO_LINES', @StepStartUTC
        );
        SET @Log_ID = CONVERT(bigint, SCOPE_IDENTITY());

        ;WITH hdr AS
        (
            SELECT h.EPOS_Fusion_Key
            FROM CTL.EPOS_Header h
            WHERE
                h.Integration_Status = 'STAGED'
                AND (@File_ID IS NULL OR h.Source_File_ID = @File_ID)
                AND (@Year IS NULL OR h.[Year] = @Year)
                AND (@Week IS NULL OR h.[Week] = @Week)
        ),
        rc AS
        (
            SELECT
                  h.EPOS_Fusion_Key
                , SUM(CASE WHEN l.Integration_Status = 'READY' THEN 1 ELSE 0 END) AS ReadyLineCount
            FROM hdr h
            LEFT JOIN CTL.EPOS_Line_Item l
              ON l.EPOS_Fusion_Key = h.EPOS_Fusion_Key
            GROUP BY h.EPOS_Fusion_Key
        )
        UPDATE h
        SET h.Integration_Status = CASE WHEN rc.ReadyLineCount > 0 THEN 'READY' ELSE 'NO_LINES' END
        FROM CTL.EPOS_Header h
        JOIN rc
          ON rc.EPOS_Fusion_Key = h.EPOS_Fusion_Key;

        SET @RowsAffected = @@ROWCOUNT;

        UPDATE EXC.Process_Transaction_Log
        SET Execution_Status = 'SUCCESS',
            Rows_Affected = @RowsAffected,
            End_UTC = SYSUTCDATETIME(),
            Message = CONCAT('Headers updated READY/NO_LINES: ', @RowsAffected)
        WHERE Log_ID = @Log_ID;
    END TRY
    BEGIN CATCH
        UPDATE EXC.Process_Transaction_Log
        SET Execution_Status = 'FAILED',
            Severity_Level = 'ERROR',
            Error_Message = ERROR_MESSAGE(),
            End_UTC = SYSUTCDATETIME()
        WHERE Log_ID = @Log_ID;
        THROW;
    END CATCH;

    SELECT @Run_ID AS Run_ID, @Txn_ID AS Txn_ID, @Correlation_ID AS Correlation_ID, 'SUCCESS' AS Result;
END;
GO
