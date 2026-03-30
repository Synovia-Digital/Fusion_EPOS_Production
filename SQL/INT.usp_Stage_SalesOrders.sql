-- =============================================================
--  Procedure : [INT].[usp_Stage_SalesOrders]
--  Database  : Fusion_EPOS_Production
--  Exported  : 2026-03-30 02:45:06
--  Created   : 2026-02-02 15:24:04.023000
--  Modified  : 2026-02-02 16:18:02.403000
-- =============================================================
USE [Fusion_EPOS_Production]
GO

CREATE OR ALTER PROCEDURE [INT].[usp_Stage_SalesOrders]
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
        BEGIN TRAN;

        /* =========================================================
           1. Default any unprocessed source rows
        ========================================================= */
        UPDATE CTL.EPOS_Line_Item
        SET Integration_Status = 'Pending'
        WHERE Integration_Status IS NULL;

        UPDATE CTL.EPOS_Header
        SET Integration_Status = 'Pending'
        WHERE Integration_Status IS NULL;

        /* =========================================================
           2. REMOVE invalid quantities (<= 0)
        ========================================================= */
        INSERT INTO EXC.Processed_Lines
        (
            EPOS_Fusion_Key,
            EPOS_Line_ID,
            CustomersOrderReference,
            ItemNumber,
            Original_Quantity,
            Processed_Action,
            Reason
        )
        SELECT
            l.EPOS_Fusion_Key,
            l.EPOS_Line_ID,
            h.Docket_Reference,
            l.Dynamics_Code,
            l.Units_Sold,
            'Removed',
            CASE 
                WHEN l.Units_Sold = 0 THEN 'Quantity = 0'
                ELSE 'Negative quantity'
            END
        FROM CTL.EPOS_Line_Item l
        JOIN CTL.EPOS_Header h
            ON h.EPOS_Fusion_Key = l.EPOS_Fusion_Key
        WHERE l.Units_Sold <= 0
          AND l.Integration_Status <> 'Removed';

        UPDATE l
        SET Integration_Status = 'Removed'
        FROM CTL.EPOS_Line_Item l
        WHERE l.Units_Sold <= 0;

        /* =========================================================
           3. AGGREGATE duplicate products per order
        ========================================================= */
        ;WITH Dupes AS
        (
            SELECT
                h.Docket_Reference AS CustomersOrderReference,
                l.Dynamics_Code AS ItemNumber,
                SUM(l.Units_Sold) AS TotalQty,
                SUM(l.Sales_Value_Inc_VAT) AS TotalAmount
            FROM CTL.EPOS_Line_Item l
            JOIN CTL.EPOS_Header h
                ON h.EPOS_Fusion_Key = l.EPOS_Fusion_Key
            WHERE l.Units_Sold > 0
              AND l.Integration_Status NOT IN ('Removed')
            GROUP BY
                h.Docket_Reference,
                l.Dynamics_Code
            HAVING COUNT(*) > 1
        )
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
            Integration_Status
        )
        SELECT
            'jbro',
            d.CustomersOrderReference,
            h.Fusion_Code,
            h.Account,
            CAST(GETDATE() AS DATE),
            d.ItemNumber,
            d.TotalQty,
            'EA',
            1,
            d.TotalAmount,
            CAST(GETDATE() AS DATE),
            'Ready_to_Transmit'
        FROM Dupes d
        JOIN CTL.EPOS_Header h
            ON h.Docket_Reference = d.CustomersOrderReference;

        /* ---- Log aggregated source lines ---- */
        INSERT INTO EXC.Processed_Lines
        (
            EPOS_Fusion_Key,
            EPOS_Line_ID,
            CustomersOrderReference,
            ItemNumber,
            Original_Quantity,
            Processed_Action,
            Reason
        )
        SELECT
            l.EPOS_Fusion_Key,
            l.EPOS_Line_ID,
            h.Docket_Reference,
            l.Dynamics_Code,
            l.Units_Sold,
            'Aggregated',
            'Duplicate product aggregated into single order line'
        FROM CTL.EPOS_Line_Item l
        JOIN CTL.EPOS_Header h
            ON h.EPOS_Fusion_Key = l.EPOS_Fusion_Key
        WHERE EXISTS (
            SELECT 1
            FROM CTL.EPOS_Line_Item x
            WHERE x.EPOS_Fusion_Key = l.EPOS_Fusion_Key
              AND x.Dynamics_Code = l.Dynamics_Code
            GROUP BY x.EPOS_Fusion_Key, x.Dynamics_Code
            HAVING COUNT(*) > 1
        )
        AND l.Integration_Status NOT IN ('Removed');

        UPDATE l
        SET Integration_Status = 'Aggregated'
        FROM CTL.EPOS_Line_Item l
        WHERE EXISTS (
            SELECT 1
            FROM CTL.EPOS_Line_Item x
            WHERE x.EPOS_Fusion_Key = l.EPOS_Fusion_Key
              AND x.Dynamics_Code = l.Dynamics_Code
              AND x.EPOS_Line_ID <> l.EPOS_Line_ID
        )
        AND l.Integration_Status NOT IN ('Removed');

        /* =========================================================
           4. STAGE clean, non-duplicate lines
        ========================================================= */
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
            Integration_Status
        )
        SELECT
            'jbro',
            h.Docket_Reference,
            h.Fusion_Code,
            h.Account,
            CAST(GETDATE() AS DATE),
            l.Dynamics_Code,
            l.Units_Sold,
            'EA',
            ROW_NUMBER() OVER (PARTITION BY h.Docket_Reference ORDER BY l.EPOS_Line_ID),
            l.Sales_Value_Inc_VAT,
            CAST(GETDATE() AS DATE),
            'Ready_to_Transmit'
        FROM CTL.EPOS_Line_Item l
        JOIN CTL.EPOS_Header h
            ON h.EPOS_Fusion_Key = l.EPOS_Fusion_Key
        WHERE l.Units_Sold > 0
          AND l.Integration_Status = 'Pending'
          AND NOT EXISTS (
              SELECT 1
              FROM CTL.EPOS_Line_Item x
              WHERE x.EPOS_Fusion_Key = l.EPOS_Fusion_Key
                AND x.Dynamics_Code = l.Dynamics_Code
              GROUP BY x.EPOS_Fusion_Key, x.Dynamics_Code
              HAVING COUNT(*) > 1
          );

        UPDATE l
        SET Integration_Status = 'Staged'
        FROM CTL.EPOS_Line_Item l
        WHERE l.Integration_Status = 'Pending';

        UPDATE h
        SET Integration_Status = 'Staged'
        FROM CTL.EPOS_Header h
        WHERE EXISTS (
            SELECT 1
            FROM CTL.EPOS_Line_Item l
            WHERE l.EPOS_Fusion_Key = h.EPOS_Fusion_Key
              AND l.Integration_Status = 'Staged'
        );

        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK;

        THROW;
    END CATCH
END;
GO
