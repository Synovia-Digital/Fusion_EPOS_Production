-- =============================================================
--  Procedure : [cur].[usp_Load_FactWeeklyRetailSales]
--  Database  : Fusion_EPOS_Production
--  Exported  : 2026-03-30 02:45:06
--  Created   : 2026-02-16 21:16:00.290000
--  Modified  : 2026-02-16 21:16:00.290000
-- =============================================================
USE [Fusion_EPOS_Production]
GO

CREATE OR ALTER PROCEDURE CUR.usp_Load_FactWeeklyRetailSales
AS
BEGIN

    SET NOCOUNT ON;

    /*
        Populate CUR.FactWeeklyRetailSales
        - Uses validated staged lines only
        - Inserts only weeks not already loaded
    */

    ;WITH SourceData AS
    (
        SELECT
            c.CalendarKey,
            f.Year AS RawYear,
            f.Week AS RawWeek,
            h.Store,
            h.Store_Name,
            p.Dynamics_Code,
            r.Dunnes_Prod_Code,
            r.Product_Description,
            SUM(r.Units_Sold) AS Retail_Units,
            SUM(r.Sales_Value_Inc_VAT) AS Retail_Value
        FROM RAW.EPOS_File f
        INNER JOIN CFG.Calenders c
            ON c.Dunnes_Year =
                CASE
                    WHEN f.Year < 100 THEN 2000 + f.Year
                    ELSE f.Year
                END
            AND c.Dunnes_Week = f.Week

        INNER JOIN CTL.EPOS_Header h
            ON h.Source_File_ID = f.File_ID

        INNER JOIN CTL.EPOS_Line_Item l
            ON l.EPOS_Fusion_Key = h.EPOS_Fusion_Key
           AND l.Integration_Status = 'SO_STAGED'

        INNER JOIN RAW.EPOS_LineItems r
            ON r.LineItem_ID = l.Source_LineItem_ID

        INNER JOIN CFG.Products p
            ON p.Dunnes_Prod_Code = r.Dunnes_Prod_Code

        WHERE f.Status = 'Completed'

        GROUP BY
            c.CalendarKey,
            f.Year,
            f.Week,
            h.Store,
            h.Store_Name,
            p.Dynamics_Code,
            r.Dunnes_Prod_Code,
            r.Product_Description
    )

    MERGE CUR.FactWeeklyRetailSales AS Target
    USING SourceData AS Source
        ON Target.CalendarKey = Source.CalendarKey
       AND Target.Store = Source.Store
       AND Target.Dynamics_Code = Source.Dynamics_Code

    WHEN NOT MATCHED BY TARGET THEN
        INSERT
        (
            CalendarKey,
            Dunnes_Year,
            Dunnes_Week,
            Store,
            Store_Name,
            Dynamics_Code,
            Dunnes_Prod_Code,
            Product_Description,
            Retail_Units,
            Retail_Value,
            InsertedAt
        )
        VALUES
        (
            Source.CalendarKey,
            CASE WHEN Source.RawYear < 100 THEN 2000 + Source.RawYear ELSE Source.RawYear END,
            Source.RawWeek,
            Source.Store,
            Source.Store_Name,
            Source.Dynamics_Code,
            Source.Dunnes_Prod_Code,
            Source.Product_Description,
            Source.Retail_Units,
            Source.Retail_Value,
            GETDATE()
        );

END;
GO
