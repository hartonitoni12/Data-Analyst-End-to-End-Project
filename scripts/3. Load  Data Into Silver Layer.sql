/*
===================================================================
Loading Bronze Layer
===================================================================

Script Purpose:
    This stored procedure loads data from source files into the bronze layer by:
    - Truncating all bronze tables
    - Inserting data using BULK INSERT

Parameters: None

Usage: EXEC bronze.sp_load_bronze
*/

CREATE OR ALTER PROCEDURE bronze.sp_load_bronze AS
BEGIN
    DECLARE 
        @start_time        DATETIME,
        @end_time          DATETIME,
        @batch_start_time  DATETIME,
        @batch_end_time    DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '=========================================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '=========================================================================';

        -- CRM Data Load
        PRINT '-------------------------------------------------------------------------';
        PRINT 'Loading CRM Data';
        PRINT '-------------------------------------------------------------------------';

        -- crm_cust_info
        PRINT '>> Truncating bronze.crm_cust_info';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting into bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\harto\OneDrive\Dokumen\SQL Course\PROJECT\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' second';
        PRINT '******************************************';

        -- crm_prd_info
        PRINT '>> Truncating bronze.crm_prd_info';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting into bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\harto\OneDrive\Dokumen\SQL Course\PROJECT\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' second';
        PRINT '******************************************';

        -- crm_sales_details
        PRINT '>> Truncating bronze.crm_sales_details';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting into bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\harto\OneDrive\Dokumen\SQL Course\PROJECT\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' second';
        PRINT '******************************************';

        -- ERP Data Load
        PRINT '-------------------------------------------------------------------------';
        PRINT 'Loading ERP Data';
        PRINT '-------------------------------------------------------------------------';

        -- erp_cust_az12
        PRINT '>> Truncating bronze.erp_cust_az12';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting into bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\harto\OneDrive\Dokumen\SQL Course\PROJECT\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' second';
        PRINT '******************************************';

        -- erp_loc_a101
        PRINT '>> Truncating bronze.erp_loc_a101';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting into bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\harto\OneDrive\Dokumen\SQL Course\PROJECT\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' second';
        PRINT '******************************************';

        -- erp_px_cat_g1v2
        PRINT '>> Truncating bronze.erp_px_cat_g1v2';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting into bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\harto\OneDrive\Dokumen\SQL Course\PROJECT\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' second';
        PRINT '******************************************';

        SET @batch_end_time = GETDATE();
        PRINT '=========================================================================';
        PRINT 'Loading Bronze Layer is complete';
        PRINT 'Load time: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' second';
        PRINT '=========================================================================';
    END TRY

    BEGIN CATCH
        PRINT '=========================================================================';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State  : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '=========================================================================';
    END CATCH
END

