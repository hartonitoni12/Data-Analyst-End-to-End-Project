/*
===================================================================
Loading Silver Layer
===================================================================
Script Purpose:
    Load data into the silver layer from the bronze layer:
    - Truncate all silver tables
    - Insert cleaned and transformed data

Usage: EXEC silver.sp_load_silver
*/

CREATE OR ALTER PROCEDURE silver.sp_load_silver AS
BEGIN
    DECLARE 
        @start_time        DATETIME,
        @end_time          DATETIME,
        @batch_start_time  DATETIME,
        @batch_end_time    DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '=========================================================================';
        PRINT 'Loading Silver Layer';
        PRINT '=========================================================================';

        -------------------------------------------------------------------------
        -- CRM Data Load
        -------------------------------------------------------------------------
        PRINT '-------------------------------------------------------------------------';
        PRINT 'Loading CRM Data';
        PRINT '-------------------------------------------------------------------------';

        -- crm_cust_info
        PRINT '>> Truncating silver.crm_cust_info';
        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting into silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id,           cst_key,          cst_firstname,
            cst_lastname,     cst_marital_status, cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname)  AS cst_lastname,
            CASE
                WHEN cst_marital_status = 'S' THEN 'Single'
                WHEN cst_marital_status = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE
                WHEN cst_gndr = 'F' THEN 'Female'
                WHEN cst_gndr = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT *, RANK() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rnk
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) s
        WHERE rnk = 1;

        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' second';
        PRINT '******************************************';

        -- crm_prd_info
        PRINT '>> Truncating silver.crm_prd_info';
        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting into silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            prd_id,        prd_cat_id,    prd_key, 
            prd_nm,        prd_cost,      prd_line, 
            prd_start_dt,  prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')                     AS prd_cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key))                             AS prd_key,
            prd_nm,
            ISNULL(prd_cost, 0)                                             AS prd_cost,
            CASE
                WHEN TRIM(prd_line) = 'M' THEN 'Mountain'
                WHEN TRIM(prd_line) = 'R' THEN 'Road'
                WHEN TRIM(prd_line) = 'T' THEN 'Touring'
                WHEN TRIM(prd_line) = 'S' THEN 'Other Sales'
                ELSE 'n/a'
            END                                                             AS prd_line,
            CAST(prd_start_dt AS DATE)                                     AS prd_start_dt,
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' second';
        PRINT '******************************************';

        -- crm_sales_details
        PRINT '>> Truncating silver.crm_sales_details';
        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting into silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,  sls_prd_key,  sls_cust_id,
            sls_order_dt, sls_ship_dt,  sls_due_dt,
            sls_sales,    sls_quantity, sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE 
                WHEN sls_order_dt = 0 OR LEN(sls_order_dt) <> 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE)
            END AS sls_order_dt,
            CASE 
                WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE 
                WHEN sls_due_dt = 0 OR LEN(sls_due_dt) <> 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE)
            END AS sls_due_dt,
            CASE 
                WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales <> sls_price * sls_quantity THEN ABS(sls_price * sls_quantity)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE 
                WHEN sls_price = 0 OR sls_price IS NULL THEN ABS(sls_sales / NULLIF(sls_quantity, 0))
                WHEN sls_price < 0 THEN ABS(sls_price)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' second';
        PRINT '******************************************';

        -------------------------------------------------------------------------
        -- ERP Data Load
        -------------------------------------------------------------------------
        PRINT '-------------------------------------------------------------------------';
        PRINT 'Loading ERP Data';
        PRINT '-------------------------------------------------------------------------';

        -- erp_cust_az12
        PRINT '>> Truncating silver.erp_cust_az12';
        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting into silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (
            cid, bdate, gen
        )
        SELECT
            CASE 
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END AS cid,
            CASE 
                WHEN bdate >= GETDATE() THEN NULL
                ELSE bdate
            END AS bdate,
            CASE 
                WHEN TRIM(gen) = 'F' THEN 'Female'
                WHEN TRIM(gen) = 'M' THEN 'Male'
                WHEN TRIM(gen) = '' OR TRIM(gen) IS NULL THEN 'n/a'
                ELSE TRIM(gen)
            END AS gen
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' second';
        PRINT '******************************************';

        -- erp_loc_a101
        PRINT '>> Truncating silver.erp_loc_a101';
        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Inserting into silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (
            cid, cntry
        )
        SELECT
            REPLACE(cid, '-', '') AS cid,
            CASE 
                WHEN TRIM(cntry) IN ('US', 'USA', 'United States') THEN 'United States'
                WHEN TRIM(cntry) IN ('DE', 'Germany')              THEN 'Germany'
                WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL       THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' second';
        PRINT '******************************************';

        -- erp_px_cat_g1v2
        PRINT '>> Truncating silver.erp_px_cat_g1v2';
        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Inserting into silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2 (
            id, cat, subcat, maintanance
        )
        SELECT
            id, cat, subcat, maintanance
        FROM bronze.erp_px_cat_g1v2;

        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' second';
        PRINT '******************************************';

        SET @batch_end_time = GETDATE();
        PRINT '=========================================================================';
        PRINT 'Loading silver Layer is complete';
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
