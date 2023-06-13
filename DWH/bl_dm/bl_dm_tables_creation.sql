CREATE OR REPLACE PROCEDURE bl_cl.tables_dm_creation()
LANGUAGE plpgsql
AS $$
BEGIN 

			CREATE TABLE IF NOT EXISTS bl_dm.dim_dates (
			  full_date 				DATE		PRIMARY KEY,
			  day_name 					VARCHAR(10) NOT NULL,
			  day_of_month 				INT 		NOT NULL,
			  day_of_week 				INT 		NOT NULL,
			  day_of_quarter 			INT 		NOT NULL,
			  day_of_year 				INT 		NOT NULL,
			  week_of_month 			INT 		NOT NULL,
			  week_of_year 				INT 		NOT NULL,
			  month_number 				INT 		NOT NULL,
			  month_name 				VARCHAR(10) NOT NULL,
			  month_name_abbr 			CHAR(3) 	NOT NULL,
			  quarter_number 			INT 		NOT NULL,
			  quarter_name 				VARCHAR(10) NOT NULL,
			  year_number 				INT 		NOT NULL,
			  first_day_of_week 		DATE 		NOT NULL,
			  last_day_of_week 			DATE 		NOT NULL,
			  first_day_of_month 		DATE 		NOT NULL,
			  last_day_of_month 		DATE 		NOT NULL,
			  first_day_of_quarter 		DATE 		NOT NULL,
			  last_day_of_quarter 		DATE 		NOT NULL,
			  first_day_of_year 		DATE 		NOT NULL,
			  last_day_of_year 			DATE 		NOT NULL,
			  mmyyyy 					CHAR(6) 	NOT NULL,
			  mmddyyyy 					CHAR(10) 	NOT NULL
			);
			
			------------------------------------------------------------------------------------------------
			-- 										DIM_PROMOTIONS
			------------------------------------------------------------------------------------------------
			
			
			CREATE TABLE IF NOT EXISTS bl_dm.dim_promotions
			  (
			     promotion_surr_id			INTEGER PRIMARY KEY,
			     promotion_id				VARCHAR(255) NOT NULL,
			     source_system				VARCHAR(50) NOT NULL,
			     source_table				VARCHAR(50) NOT NULL,
			     promotion_channel			VARCHAR(100) NOT NULL,
			     promotion_desc				VARCHAR(1500) NOT NULL, 
			     insert_dt              	TIMESTAMP NOT NULL,
			     update_dt              	TIMESTAMP NOT NULL
			  ); 
			 
			------------------------------------------------------------------------------------------------
			-- 										DIM_EMPLOYEES
			------------------------------------------------------------------------------------------------
			
			
			CREATE TABLE IF NOT EXISTS bl_dm.dim_employees
			  (
			     employee_surr_id     		INTEGER PRIMARY KEY,
			     employee_id				VARCHAR(50) NOT NULL, 
			     source_system				VARCHAR(50) NOT NULL,
			     source_table				VARCHAR(255) NOT NULL,
			     employee_first_name		VARCHAR(255) NOT NULL,
			     employee_last_name			VARCHAR(255) NOT NULL,
			     employee_full_name			VARCHAR(255) NOT NULL,
			     employee_email				VARCHAR(255) NOT NULL,
			     employee_phone				VARCHAR(255) NOT NULL,
			     employee_salary			VARCHAR(255) NOT NULL,
			     insert_dt					TIMESTAMP NOT NULL,
			     update_dt					TIMESTAMP NOT NULL 
			  ); 
			
			------------------------------------------------------------------------------------------------
			-- 										DIM_WAREHOUSES
			------------------------------------------------------------------------------------------------
			
			
			CREATE TABLE IF NOT EXISTS bl_dm.dim_warehouses
			  (
			     warehouse_surr_id   			INTEGER PRIMARY KEY,
			     warehouse_id					VARCHAR(50) NOT NULL, 
			     source_system					VARCHAR(50) NOT NULL,
			     source_table					VARCHAR(255) NOT NULL,
			     warehouse_city_id				INTEGER NOT NULL,
			     warehouse_city_name			VARCHAR(50) NOT NULL, 
			     warehouse_region_id			INTEGER NOT NULL,
			     warehouse_region_name			VARCHAR(50) NOT NULL,
			     warehouse_country_id			INTEGER	NOT NULL, 
			     warehouse_country_name			VARCHAR(50) NOT NULL,
			     address_id						INTEGER NOT NULL, 
			     warehouse_area					DECIMAL NOT NULL,
			     warehouse_number_of_workers	INTEGER NOT NULL, 		
			     insert_dt						TIMESTAMP NOT NULL,
			     update_dt						TIMESTAMP NOT NULL 
			  ); 
			 
			 
			------------------------------------------------------------------------------------------------
			-- 										DIM_DELIVERIES
			------------------------------------------------------------------------------------------------
			
			
			CREATE TABLE IF NOT EXISTS bl_dm.dim_deliveries
			  (
			     delivery_surr_id			INTEGER PRIMARY KEY,
			     delivery_id				VARCHAR(50) NOT NULL,
			     source_system				VARCHAR(50) NOT NULL,
			     source_table				VARCHAR(255) NOT NULL,
			     delivery_type_name			VARCHAR(255) NOT NULL,
			     delivery_details			VARCHAR(500) NOT NULL, 
			     insert_dt              	TIMESTAMP NOT NULL,
			     update_dt              	TIMESTAMP NOT NULL
			  );
			
			------------------------------------------------------------------------------------------------
			-- 										DIM_PRODUCTS_SCD
			------------------------------------------------------------------------------------------------
			
			
			CREATE TABLE IF NOT EXISTS bl_dm.dim_products
				(
				 product_surr_id			INTEGER PRIMARY KEY,
				 product_id					VARCHAR(50) NOT NULL,
				 source_system				VARCHAR(50) NOT NULL,
				 source_table				VARCHAR(255) NOT NULL,
				 product_name				VARCHAR(255) NOT NULL,
				 product_category_id		INTEGER NOT NULL,
				 product_category    		VARCHAR(255) NOT NULL, 
				 is_active					BOOLEAN NOT NULL, 
				 start_dt 					DATE NOT NULL,
				 end_dt						DATE NOT NULL
				);
			
			------------------------------------------------------------------------------------------------
			-- 										DIM_CUSTOMERS
			------------------------------------------------------------------------------------------------
			
			
			CREATE TABLE IF NOT EXISTS bl_dm.dim_customers
				(
				 customer_surr_id			INTEGER PRIMARY KEY,
				 customer_id				VARCHAR(50) NOT NULL,
				 source_system				VARCHAR(50) NOT NULL,
				 source_table				VARCHAR(255) NOT NULL,
				 customer_first_name		VARCHAR(255) NOT NULL,
				 customer_last_name			VARCHAR(255) NOT NULL,
				 customer_full_name			VARCHAR(255) NOT NULL,
				 customer_email				VARCHAR(255) NOT NULL,
				 customer_phone				VARCHAR(255) NOT NULL,
				 insert_dt					TIMESTAMP NOT NULL,
				 update_dt					TIMESTAMP NOT NULL 			
				);
			------------------------------------------------------------------------------------------------
			-- 										DIM_PAYMENT_TYPES
			------------------------------------------------------------------------------------------------
			
			
			CREATE TABLE IF NOT EXISTS bl_dm.dim_payment_types
				(
					payment_type_surr_id	INTEGER			NOT NULL PRIMARY KEY,
					payment_type_id			VARCHAR(20)		NOT NULL,
					source_system			VARCHAR(50)		NOT NULL,
					source_table			VARCHAR(50)		NOT NULL,
					payment_type			VARCHAR(20)		NOT NULL, 
					insert_dt				TIMESTAMP 		NOT NULL,
					update_dt				TIMESTAMP		NOT NULL 
				);
			
			
			------------------------------------------------------------------------------------------------
			-- 										FCT_SALES
			------------------------------------------------------------------------------------------------
			
			CREATE TABLE IF NOT EXISTS bl_dm.fct_sales
				(
				 delivery_surr_id		INTEGER NOT NULL,
				 product_surr_id		INTEGER NOT NULL,
				 customer_surr_id		INTEGER NOT NULL,
				 employee_surr_id		INTEGER NOT NULL,
				 warehouse_surr_id		INTEGER NOT NULL,
				 promotion_surr_id		INTEGER NOT NULL,
				 payment_type_surr_id	INTEGER NOT NULL,
				 sale_dt				DATE 	NOT NULL,
				 quantity				INTEGER NOT NULL,
				 actual_price			DECIMAL(10,2) NOT NULL, 
				 discount_price			DECIMAL(10,2) NOT NULL,
				 insert_dt				TIMESTAMP NOT NULL 
				) PARTITION BY RANGE (sale_dt);
			
END; $$;