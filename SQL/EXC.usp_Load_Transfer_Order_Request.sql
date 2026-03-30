-- =============================================================
--  Procedure : [EXC].[usp_Load_Transfer_Order_Request]
--  Database  : Fusion_EPOS_Production
--  Exported  : 2026-03-30 02:45:06
--  Created   : 2026-02-18 12:33:42.523000
--  Modified  : 2026-02-18 12:38:57.460000
-- =============================================================
USE [Fusion_EPOS_Production]
GO

CREATE OR ALTER PROCEDURE [EXC].[usp_Load_Transfer_Order_Request]
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @Today DATE = CAST(SYSUTCDATETIME() AS DATE);

    BEGIN TRY
        BEGIN TRAN;

        /* =========================================================
           1) Build working sets (temp tables) for reuse
           ========================================================= */

        IF OBJECT_ID('tempdb..#Orders') IS NOT NULL DROP TABLE #Orders;
        CREATE TABLE #Orders
        (
            OrderId NVARCHAR(50) NOT NULL PRIMARY KEY,
            CustomerCode NVARCHAR(50) NULL
        );

        INSERT INTO #Orders (OrderId, CustomerCode)
        SELECT
            r.OrderId,
            MAX(r.CustomerCode) AS CustomerCode
        FROM RAW.EPOS_TransferRequest r
        WHERE r.SynProcessed = 0
          AND r.SynStaged = 0
          AND NULLIF(LTRIM(RTRIM(r.OrderId)), '') IS NOT NULL
        GROUP BY r.OrderId;

        -- nothing to do
        IF NOT EXISTS (SELECT 1 FROM #Orders)
        BEGIN
            COMMIT;
            RETURN;
        END

        /* =========================================================
           2) Upsert EXC.TransferOrder_Control
           ========================================================= */
        ;WITH Mapped AS
        (
            SELECT
                o.OrderId,
                o.CustomerCode,
                NULLIF(LTRIM(RTRIM(ds.Warehouse)), '') AS ReceivingWarehouseId,
                CASE
                    WHEN ds.Account IS NULL THEN 0
                    WHEN NULLIF(LTRIM(RTRIM(ds.Warehouse)), '') IS NULL THEN 0
                    ELSE 1
                END AS ReadyForProcessing,
                CASE
                    WHEN ds.Account IS NULL THEN CONCAT('Customer not mapped in CFG.Dynamics_Stores. CustomerCode=', ISNULL(o.CustomerCode,'(NULL)'))
                    WHEN NULLIF(LTRIM(RTRIM(ds.Warehouse)), '') IS NULL THEN CONCAT('Warehouse blank/not mapped in CFG.Dynamics_Stores for CustomerCode=', ISNULL(o.CustomerCode,'(NULL)'))
                    ELSE NULL
                END AS ValidationMessage
            FROM #Orders o
            LEFT JOIN CFG.Dynamics_Stores ds
                ON ds.Account = o.CustomerCode
        )
        MERGE EXC.TransferOrder_Control AS tgt
        USING
        (
            SELECT
                CONCAT('ORDER_', m.OrderId) AS SourceID,
                m.OrderId,
                m.CustomerCode,
                '010' AS ShippingWarehouseId,
                m.ReceivingWarehouseId,
                @Today AS RequestedShippingDate,
                @Today AS RequestedReceiptDate,
                m.ReadyForProcessing,
                m.ValidationMessage
            FROM Mapped m
        ) AS src
        ON tgt.SourceID = src.SourceID
        WHEN MATCHED THEN
            UPDATE SET
                tgt.OrderId = src.OrderId,
                tgt.CustomerCode = src.CustomerCode,
                tgt.ShippingWarehouseId = src.ShippingWarehouseId,
                tgt.ReceivingWarehouseId = src.ReceivingWarehouseId,
                tgt.RequestedShippingDate = src.RequestedShippingDate,
                tgt.RequestedReceiptDate = src.RequestedReceiptDate,
                tgt.ReadyForProcessing = src.ReadyForProcessing,
                tgt.ValidationMessage = src.ValidationMessage,
                tgt.MasterStatus =
                    CASE WHEN src.ReadyForProcessing = 1 THEN 'VALIDATED' ELSE 'NEW' END,
                tgt.StatusUpdatedAt = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT
            (
                SourceID, OrderId, CustomerCode,
                ShippingWarehouseId, ReceivingWarehouseId,
                RequestedShippingDate, RequestedReceiptDate,
                ReadyForProcessing, ValidationMessage,
                MasterStatus, StatusUpdatedAt
            )
            VALUES
            (
                src.SourceID, src.OrderId, src.CustomerCode,
                src.ShippingWarehouseId, src.ReceivingWarehouseId,
                src.RequestedShippingDate, src.RequestedReceiptDate,
                src.ReadyForProcessing, src.ValidationMessage,
                CASE WHEN src.ReadyForProcessing = 1 THEN 'VALIDATED' ELSE 'NEW' END,
                SYSUTCDATETIME()
            );

        /* =========================================================
           3) Map OrderId -> TransferControlId for downstream inserts
           ========================================================= */
        IF OBJECT_ID('tempdb..#OrderToControl') IS NOT NULL DROP TABLE #OrderToControl;
        CREATE TABLE #OrderToControl
        (
            OrderId NVARCHAR(50) NOT NULL PRIMARY KEY,
            TransferControlId BIGINT NOT NULL
        );

        INSERT INTO #OrderToControl (OrderId, TransferControlId)
        SELECT
            c.OrderId,
            c.TransferControlId
        FROM EXC.TransferOrder_Control c
        INNER JOIN #Orders o
            ON o.OrderId = c.OrderId;

        /* =========================================================
           4) Insert INT.TransferOrderHeader (READY only)
           ========================================================= */
        INSERT INTO INT.TransferOrderHeader
        (
            TransferControlId,
            SourceID,
            ShippingWarehouseId,
            ReceivingWarehouseId,
            RequestedShippingDate,
            RequestedReceiptDate
        )
        SELECT
            c.TransferControlId,
            c.SourceID,
            c.ShippingWarehouseId,
            c.ReceivingWarehouseId,
            c.RequestedShippingDate,
            c.RequestedReceiptDate
        FROM EXC.TransferOrder_Control c
        INNER JOIN #Orders o
            ON o.OrderId = c.OrderId
        WHERE c.ReadyForProcessing = 1
          AND c.MasterStatus = 'VALIDATED'
          AND NOT EXISTS
          (
              SELECT 1
              FROM INT.TransferOrderHeader h
              WHERE h.TransferControlId = c.TransferControlId
          );

        /* =========================================================
           5) Insert INT.TransferOrderLine (READY only)
              Aggregate by ProductCode, sum quantities
           ========================================================= */
        ;WITH ReadyOrders AS
        (
            SELECT otc.OrderId, otc.TransferControlId
            FROM #OrderToControl otc
            INNER JOIN EXC.TransferOrder_Control c
                ON c.TransferControlId = otc.TransferControlId
            WHERE c.ReadyForProcessing = 1
              AND c.MasterStatus = 'VALIDATED'
        ),
        LineAgg AS
        (
            SELECT
                ro.TransferControlId,
                r.ProductCode,
                SUM(COALESCE(TRY_CONVERT(DECIMAL(18,4), r.Quantity), 0)) AS Qty
            FROM ReadyOrders ro
            JOIN RAW.EPOS_TransferRequest r
                ON r.OrderId = ro.OrderId
            WHERE r.SynProcessed = 0
              AND r.SynStaged = 0
              AND NULLIF(LTRIM(RTRIM(r.ProductCode)), '') IS NOT NULL
            GROUP BY ro.TransferControlId, r.ProductCode
        ),
        LineNumbered AS
        (
            SELECT
                la.TransferControlId,
                ROW_NUMBER() OVER (PARTITION BY la.TransferControlId ORDER BY la.ProductCode) AS [LineNo],
                la.ProductCode,
                la.Qty
            FROM LineAgg la
            WHERE la.Qty <> 0
        )
        INSERT INTO INT.TransferOrderLine
        (
            TransferControlId,
            [LineNo],
            ProductCode,
            Quantity
        )
        SELECT
            ln.TransferControlId,
            ln.[LineNo],
            ln.ProductCode,
            ln.Qty
        FROM LineNumbered ln
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM INT.TransferOrderLine l
            WHERE l.TransferControlId = ln.TransferControlId
              AND l.ProductCode = ln.ProductCode
        );

        /* =========================================================
           6) Mark RAW as staged for those orders (ready or not)
           ========================================================= */
        UPDATE r
        SET
            r.SynStaged = 1,
            r.SynStagedDate = SYSUTCDATETIME()
        FROM RAW.EPOS_TransferRequest r
        INNER JOIN #Orders o
            ON o.OrderId = r.OrderId
        WHERE r.SynProcessed = 0
          AND r.SynStaged = 0;

        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE
            @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE(),
            @ErrNum INT = ERROR_NUMBER(),
            @ErrState INT = ERROR_STATE(),
            @ErrLine INT = ERROR_LINE(),
            @ErrProc NVARCHAR(200) = ERROR_PROCEDURE();

        RAISERROR(
            'EXC.usp_Load_Transfer_Order_Request failed. Proc=%s Line=%d Err=%d State=%d Msg=%s',
            16, 1,
            @ErrProc, @ErrLine, @ErrNum, @ErrState, @ErrMsg
        );
    END CATCH
END
GO
