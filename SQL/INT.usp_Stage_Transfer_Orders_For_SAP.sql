-- =============================================================
--  Procedure : [INT].[usp_Stage_Transfer_Orders_For_SAP]
--  Database  : Fusion_EPOS_Production
--  Exported  : 2026-03-30 02:45:06
--  Created   : 2026-03-23 20:35:58.150000
--  Modified  : 2026-03-23 20:35:58.150000
-- =============================================================
USE [Fusion_EPOS_Production]
GO

/*
=================================================================
  [INT].[usp_Stage_Transfer_Orders_For_SAP]

  Purpose:
      Populates the SAP staging tables from the validated,
      Dynamics-confirmed transfer orders so that
      sap_cpi_transmit.py can read and transmit them.

  Source tables:
      EXC.TransferOrder_Control     (master — MasterStatus = 'SENT_DYNAMICS')
      INT.TransferOrderHeader       (shipping/receipt dates, warehouses)
      INT.TransferOrderLine         (product lines)

  Target tables:
      INT.Transfer_Order_SAP_Header
      INT.Transfer_Order_SAP_Item
      INT.Transfer_Order_SAP_Note

  Idempotent:
      Skips any SO_Number already present in
      INT.Transfer_Order_SAP_Header to prevent duplicate staging.

  Payload field mapping (from working QAS example):
      SO_Type            = 'Transfer Order'
      Sales_Pool         = 'JM'
      Division           = 'JM'
      SO_Number          = EXC.TransferOrder_Control.DynamicsDocumentNo
      Customer_Reference = EXC.TransferOrder_Control.DynamicsDocumentNo
      Customer_Code      = EXC.TransferOrder_Control.CustomerCode
      Delivery_Point     = ''  (blank per example)
      Document_Date      = INT.TransferOrderHeader.RequestedShippingDate  (ISO string)
      Delivery_Date      = INT.TransferOrderHeader.RequestedReceiptDate   (ISO string)
      Delivery_Note      = ''
      Reason_Code        = ''
      SubDivision        = ''
      Unit_Of_Measure    = 'CS'
      Warehouse          = INT.TransferOrderHeader.ShippingWarehouseId
      Site               = 'JM00'
      SAP_Status         = 'Unrestrict'
      Customer_Item      = ProductCode  (mirrors product)
      Language           = 'EN'
      Special_Delivery_Note = ''
=================================================================
*/

CREATE   PROCEDURE [INT].[usp_Stage_Transfer_Orders_For_SAP]
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @StagedHeaders  INT = 0;
    DECLARE @StagedItems    INT = 0;
    DECLARE @StagedNotes    INT = 0;
    DECLARE @Skipped        INT = 0;

    BEGIN TRY
        BEGIN TRAN;

        /* =============================================================
           1) Identify orders ready for SAP staging
              - MasterStatus = 'SENT_DYNAMICS'  (D365 confirmed)
              - SapStatus IS NULL or 'ERROR'     (not yet sent / retry)
              - DynamicsDocumentNo populated     (we need the TO number)
              - Not already staged (idempotency guard)
           ============================================================= */
        IF OBJECT_ID('tempdb..#ToStage') IS NOT NULL DROP TABLE #ToStage;

        SELECT
            c.TransferControlId,
            c.DynamicsDocumentNo        AS SO_Number,
            c.CustomerCode              AS Customer_Code,
            c.ReceivingWarehouseId      AS Delivery_Point,
            c.OrderId                   AS OrderId,
            h.ShippingWarehouseId,
            h.ReceivingWarehouseId,
            -- Format dates as nvarchar to match column type in SAP header table
            CONVERT(NVARCHAR(20),
                CAST(h.RequestedShippingDate AS DATETIME2), 126)  AS Document_Date,
            CONVERT(NVARCHAR(20),
                CAST(h.RequestedReceiptDate  AS DATETIME2), 126)  AS Delivery_Date
        INTO #ToStage
        FROM EXC.TransferOrder_Control c
        INNER JOIN INT.TransferOrderHeader h
            ON  h.TransferControlId = c.TransferControlId
        WHERE c.MasterStatus        = 'SENT_DYNAMICS'
          AND NULLIF(LTRIM(RTRIM(c.DynamicsDocumentNo)), '') IS NOT NULL
          AND (c.SapStatus IS NULL OR c.SapStatus = 'ERROR')
          -- Idempotency: skip if already staged
          AND NOT EXISTS
          (
              SELECT 1
              FROM INT.Transfer_Order_SAP_Header sh
              WHERE sh.SO_Number = c.DynamicsDocumentNo
                AND sh.SynProcessed = 0   -- allow re-stage if previously processed
          );

        -- Nothing to do
        IF NOT EXISTS (SELECT 1 FROM #ToStage)
        BEGIN
            COMMIT;
            RETURN;
        END

        /* =============================================================
           2) Insert INT.Transfer_Order_SAP_Header
           ============================================================= */
        INSERT INTO INT.Transfer_Order_SAP_Header
        (
            SO_Type,
            Document_Date,
            Sales_Pool,
            SO_Number,
            Division,
            SubDivision,
            Customer_Code,
            Delivery_Point,
            Customer_Reference,
            Delivery_Date,
            Delivery_Note,
            Reason_Code,
            SynProcessed
        )
        SELECT
            'Transfer Order'    AS SO_Type,
            ts.Document_Date,
            'JM'                AS Sales_Pool,
            ts.SO_Number,
            'JM'                AS Division,
            ''                  AS SubDivision,
            ts.Customer_Code,
            ''                  AS Delivery_Point,   -- blank per working example
            ts.SO_Number        AS Customer_Reference,
            ts.Delivery_Date,
            ''                  AS Delivery_Note,
            ''                  AS Reason_Code,
            0                   AS SynProcessed
        FROM #ToStage ts;

        SET @StagedHeaders = @@ROWCOUNT;

        /* =============================================================
           3) Map SO_Number -> TransferOrderSAPHeader_ID for FK inserts
           ============================================================= */
        IF OBJECT_ID('tempdb..#HeaderMap') IS NOT NULL DROP TABLE #HeaderMap;

        SELECT
            sh.TransferOrderSAPHeader_ID,
            ts.TransferControlId,
            ts.SO_Number,
            ts.ShippingWarehouseId
        INTO #HeaderMap
        FROM INT.Transfer_Order_SAP_Header sh
        INNER JOIN #ToStage ts
            ON ts.SO_Number = sh.SO_Number;

        /* =============================================================
           4) Insert INT.Transfer_Order_SAP_Item
              One row per line in INT.TransferOrderLine
           ============================================================= */
        INSERT INTO INT.Transfer_Order_SAP_Item
        (
            TransferOrderSAPHeader_ID,
            Line_Number,
            Product_Number,
            Quantity,
            Unit_Of_Measure,
            Warehouse,
            Site,
            SAP_Status,
            Customer_Item
        )
        SELECT
            hm.TransferOrderSAPHeader_ID,
            l.[LineNo]                      AS Line_Number,
            l.ProductCode                   AS Product_Number,
            l.Quantity,
            'CS'                            AS Unit_Of_Measure,
            hm.ShippingWarehouseId          AS Warehouse,
            'JM00'                          AS Site,
            'Unrestrict'                    AS SAP_Status,
            l.ProductCode                   AS Customer_Item
        FROM #HeaderMap hm
        INNER JOIN INT.TransferOrderLine l
            ON  l.TransferControlId = hm.TransferControlId
        WHERE l.Quantity <> 0
          AND NULLIF(LTRIM(RTRIM(l.ProductCode)), '') IS NOT NULL;

        SET @StagedItems = @@ROWCOUNT;

        /* =============================================================
           5) Insert INT.Transfer_Order_SAP_Note
              One note row per header
           ============================================================= */
        INSERT INTO INT.Transfer_Order_SAP_Note
        (
            TransferOrderSAPHeader_ID,
            Language,
            Special_Delivery_Note
        )
        SELECT
            hm.TransferOrderSAPHeader_ID,
            'EN'    AS Language,
            ''      AS Special_Delivery_Note
        FROM #HeaderMap hm;

        SET @StagedNotes = @@ROWCOUNT;

        COMMIT;

        -- Summary output (visible in Python log / SSMS messages)
        PRINT CONCAT(
            'usp_Stage_Transfer_Orders_For_SAP complete. ',
            'Headers=', @StagedHeaders,
            '  Items=',   @StagedItems,
            '  Notes=',   @StagedNotes,
            '  Skipped=', @Skipped
        );

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE
            @ErrMsg   NVARCHAR(4000) = ERROR_MESSAGE(),
            @ErrNum   INT            = ERROR_NUMBER(),
            @ErrLine  INT            = ERROR_LINE(),
            @ErrProc  NVARCHAR(200)  = ERROR_PROCEDURE();

        RAISERROR(
            'INT.usp_Stage_Transfer_Orders_For_SAP failed. Proc=%s Line=%d Err=%d Msg=%s',
            16, 1,
            @ErrProc, @ErrLine, @ErrNum, @ErrMsg
        );
    END CATCH
END;
GO
