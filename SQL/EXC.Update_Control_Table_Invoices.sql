-- =============================================================
--  Procedure : [EXC].[Update_Control_Table_Invoices]
--  Database  : Fusion_EPOS_Production
--  Exported  : 2026-03-30 02:45:06
--  Created   : 2026-02-02 20:36:23.630000
--  Modified  : 2026-02-02 22:56:29.523000
-- =============================================================
USE [Fusion_EPOS_Production]
GO

CREATE OR ALTER PROCEDURE EXC.Update_Control_Table_Invoices
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RowsUpdated INT = 0;

    BEGIN TRY
        BEGIN TRAN;

        ;WITH DistinctInvoices AS (
            SELECT
                invoice_number,
                sales_order_number,
                buyer_order_number
            FROM EDI.D365_Invoices
            GROUP BY
                invoice_number,
                sales_order_number,
                buyer_order_number
        )
        UPDATE eh
        SET
            eh.D365_Invoice_Number = di.invoice_number,
            eh.Integration_Status  = 'Invoiced'
        FROM CTL.EPOS_Header eh
        JOIN DistinctInvoices di
            ON eh.Docket_Reference = di.buyer_order_number
           AND eh.D365_SO_Number  = di.sales_order_number
        WHERE
            eh.D365_Invoice_Number IS NULL
            AND eh.Integration_Status <> 'Invoiced';

        SET @RowsUpdated = @@ROWCOUNT;

        COMMIT;

        SELECT @RowsUpdated AS Rows_Updated;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK;
        THROW;
    END CATCH
END;
GO
