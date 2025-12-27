/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	 DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
     BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		-- Loading silver.crm_cust_info
        SET @start_time = GETDATE();
		PRINT '>> Trucating the table : silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info
		PRINT '>> Inserting Data into : silver.crm_cust_info'

		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_material_status,
			cst_gndr,
			cst_create_date)

		select 
			cst_id,
			cst_key,
			Trim(cst_firstname) as cst_firstname,
			TRIM(cst_lastname) as cst_lastname,
			case when upper(Trim(cst_material_status))='S' then 'Single'
				when upper(Trim(cst_material_status))='M' then 'Married'
				else 'n/a'
				end as cst_material_status,-- Normalize marital status values to readable format
			case when upper(Trim(cst_gndr))='F' then 'Female'
				when upper(Trim(cst_gndr))='M' then 'Male'
				else 'n/a'
				end as cst_gndr,-- Normalize gender values to readable format
			cst_create_date
		from(
			select 
				*,
				row_number() over(partition by cst_id order by cst_create_date desc) as flag_last 
			from bronze.crm_cust_info) t
		where flag_last =1 and cst_id is not null;-- Select the most recent record per customer
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.crm_prd_info
        SET @start_time = GETDATE();
		PRINT '>> Trucating the table : silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info
		PRINT '>> Inserting Data into : silver.crm_prd_info'

		INSERT INTO silver.crm_prd_info(
					prd_id,
					cat_id,
					prd_key,
					prd_nm,
					prd_cost,
					prd_line,
					prd_start_dt,
					prd_end_dt
		)
		select prd_id
				,Replace(substring(prd_key,1,5),'-','_') cat_id  -- Extract category ID
				,SUBSTRING(prd_key,7, len(prd_key)) prd_key	   -- Extract product key
				,prd_nm
				,ISNULL(prd_cost,0) as prd_cost
				,CASE WHEN UPPER(TRIM(prd_line))='M' then 'Mountain'
					when UPPER(TRIM(prd_line))='R' then 'Road'
					when UPPER(TRIM(prd_line))='S' then 'Other Sales'
					when UPPER(TRIM(prd_line))='T' then 'Touring'
					else 'n/a'
				end as prd_line, -- Map product line codes to descriptive values
				cast(prd_start_dt as date) AS prd_start_dt,
				cast(
						LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt asc )-1 
						as date
						) AS prd_end_dt  -- Calculate end date as one day before the next start date
		From bronze.crm_prd_info;
		 SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading crm_sales_details
        SET @start_time = GETDATE();
		PRINT '>> Trucating the table : silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details
		PRINT '>> Inserting Data into : silver.crm_sales_details'

		INSERT INTO silver.crm_sales_details(
					sls_ord_num,
					sls_prd_key,
					sls_cust_id,
					sls_order_dt,
					sls_ship_dt,
					sls_due_dt,
					sls_sales,
					sls_quantity,
					sls_price
				)
		select sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				CASE WHEN sls_order_dt =0 or LEN(sls_order_dt)!=8 THEN NULL
					ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
				END AS sls_order_dt,
				CASE WHEN sls_ship_dt =0 or LEN(sls_ship_dt)!=8 THEN NULL
					ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
				END AS sls_ship_dt,
				CASE WHEN sls_due_dt =0 or LEN(sls_due_dt)!=8 THEN NULL
					ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
				END AS sls_due_dt,
				CASE WHEN sls_sales<=0 OR sls_sales is null or sls_sales!=sls_quantity*abs(sls_price) THEN sls_quantity*abs(sls_price) 
					else sls_sales 
					end as sls_sales,-- Recalculate sales if original value is missing or incorrect,
				sls_quantity,
				CASE	when sls_price =0 or sls_price is null then sls_sales/NULLIF(sls_quantity,0) 
					when sls_price<0 then abs(sls_price)
					else sls_price
					end as sls_price -- Derive price if original value is invalid  
		from bronze.crm_sales_details
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

        -- Loading erp_cust_az12
        SET @start_time = GETDATE();
		PRINT '>> Trucating the table : silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12
		PRINT '>> Inserting Data into : silver.erp_cust_az12'

		INSERT INTO silver.erp_cust_az12(
				cid,
				bdate,
				gen
		)
		select 
		case when cid like 'NAS%' then Substring(cid,4,len(cid))
				else cid
				end as cid, -- Remove 'NAS' prefix if present
		case when bdate <'1925-01-01' or bdate>getdate() then null
				else bdate
		end as bdate,  -- Set future birthdates to NULL
		case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
				when upper(trim(gen)) in ('M','MALE') then 'Male'
				else 'n/a'
		end as gen  -- Normalize gender values and handle unknown cases
		from bronze.erp_cust_az12 ;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading erp_loc_a101
        SET @start_time = GETDATE();
		PRINT '>> Trucating the table : silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101
		PRINT '>> Inserting Data into : silver.erp_loc_a101'

		INSERT INTO silver.erp_loc_a101(
					cid,
					cntry
		)
		select REPLACE(cid,'-','') as cid,
			case when trim(cntry) ='DE' then 'Germany'
					when trim(cntry) in ('US','USA') then 'United States'
					when trim(cntry) =''or cntry is null then 'n/a'
					else trim(cntry)
			end as cntry  -- Normalize and Handle missing or blank country codes
		from bronze.erp_loc_a101;
		 SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
		
		-- Loading erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>> Trucating the table : silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		PRINT '>> Inserting Data into : silver.erp_px_cat_g1v2'

		INSERT INTO silver.erp_px_cat_g1v2(
					id,
					cat,
					subcat,
					maintenance
		)
		select  id,
				cat,
				subcat,
				maintenance
		from bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END

exec silver.load_silver








