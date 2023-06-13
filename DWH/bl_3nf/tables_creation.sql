CREATE OR REPLACE PROCEDURE bl_cl.bl_3nf_tables_creation()
LANGUAGE plpgsql
AS $$
BEGIN 
				CREATE TABLE IF NOT EXISTS bl_3nf.ce_regions
					(
						region_id		INTEGER			NOT NULL PRIMARY KEY,  	
						region_src_id	VARCHAR(50)		NOT NULL,
						source_system	VARCHAR(50)		NOT NULL,
						source_table	VARCHAR(50)		NOT NULL,
						region_name		VARCHAR(50)		NOT NULL,
						insert_dt		TIMESTAMP		NOT NULL,
						update_dt		TIMESTAMP		NOT NULL
					);
				
				INSERT INTO bl_3nf.ce_regions
					(	
						region_id,
						region_src_id,
						source_system,
						source_table,
						region_name,
						insert_dt,
						update_dt	
					)
				VALUES (-1, 'N/A', 'MANUAL', 'MANUAL', 'N/A', '1.1.1900', '1.1.1900'); 
				
				CREATE TABLE IF NOT EXISTS bl_3nf.ce_countries
					(
						country_id		INTEGER				NOT NULL PRIMARY KEY,  	
						country_src_id	VARCHAR(50)			NOT NULL,
						source_system	VARCHAR(100)		NOT NULL,
						source_table	VARCHAR(100)		NOT NULL,
						country_name	VARCHAR(100)		NOT NULL,
						region_id		INTEGER 			NOT NULL,
						insert_dt		TIMESTAMP			NOT NULL,
						update_dt		TIMESTAMP			NOT NULL
					);
				
				INSERT INTO bl_3nf.ce_countries
					(	
						country_id,
						country_src_id,
						source_system,
						source_table,
						country_name,
						region_id,
						insert_dt,
						update_dt	
					)
				VALUES (-1, 'N/A', 'MANUAL', 'MANUAL', 'N/A', -1, '1.1.1900', '1.1.1900'); 
				
				CREATE TABLE IF NOT EXISTS bl_3nf.ce_cities
					(
						city_id			INTEGER			NOT NULL PRIMARY KEY,  	
						city_src_id		VARCHAR(50)		NOT NULL,
						source_system	VARCHAR(50)		NOT NULL,
						source_table	VARCHAR(50)		NOT NULL,
						country_id		INTEGER			NOT NULL, 
						city_name		VARCHAR(50)		NOT NULL,
						insert_dt		TIMESTAMP		NOT NULL,
						update_dt		TIMESTAMP		NOT NULL
					);
				
				INSERT INTO bl_3nf.ce_cities
					(	
						city_id,
						city_src_id,
						source_system,
						source_table,
						country_id,
						city_name,
						insert_dt,
						update_dt	
					)
				VALUES (-1, 'N/A', 'MANUAL', 'MANUAL', -1, 'N/A', '1.1.1900', '1.1.1900'); 
			
				CREATE TABLE IF NOT EXISTS bl_3nf.ce_addresses
					(
						address_id			INTEGER			NOT NULL PRIMARY KEY,  	
						address_src_id		VARCHAR(50)		NOT NULL,
						source_system		VARCHAR(50)		NOT NULL,
						source_table		VARCHAR(50)		NOT NULL,
						city_id				INTEGER			NOT NULL,
						address				VARCHAR(200)	NOT NULL,
						insert_dt			TIMESTAMP		NOT NULL,
						update_dt			TIMESTAMP		NOT NULL
					);
				
				INSERT INTO bl_3nf.ce_addresses
					(	
						address_id,
						address_src_id,
						source_system,
						source_table,
						city_id,
						address,
						insert_dt,
						update_dt
					)
				VALUES (-1, 'N/A', 'MANUAL', 'MANUAL', -1, 'N/A', '1.1.1900', '1.1.1900'); 
				
				CREATE TABLE IF NOT EXISTS bl_3nf.ce_customers
					(
						customer_id				INTEGER			NOT NULL PRIMARY KEY,
						customer_src_id			VARCHAR(50)		NOT NULL,
						source_system			VARCHAR(50)		NOT NULL,
						source_table			VARCHAR(50)		NOT NULL,
						customer_first_name		VARCHAR(255)	NOT NULL, 
						customer_last_name		VARCHAR(255)	NOT NULL, 
						customer_full_name		VARCHAR(255)	NOT NULL, 
						customer_email			VARCHAR(255)	NOT NULL,
						customer_phone			VARCHAR(255)	NOT NULL,
						insert_dt				TIMESTAMP 		NOT NULL,
						update_dt				TIMESTAMP		NOT NULL
					);
				
				INSERT INTO bl_3nf.ce_customers
					(	
						customer_id,
						customer_src_id,
						source_system,
						source_table,
						customer_first_name,
						customer_last_name,
						customer_full_name,
						customer_email,
						customer_phone,
						insert_dt,
						update_dt
					)
				VALUES (-1, 'N/A', 'MANUAL', 'MANUAL', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', '1.1.1900', '1.1.1900');
				
				CREATE TABLE IF NOT EXISTS bl_3nf.ce_warehouses
					(
					 warehouse_id					INTEGER			NOT NULL PRIMARY KEY,
					 warehouse_src_id				VARCHAR(50) 	NOT NULL,
					 source_system					VARCHAR(50)		NOT NULL,
					 source_table					VARCHAR(50)		NOT NULL,
					 address_id						INTEGER			NOT NULL,
					 warehouse_area					INTEGER			NOT NULL, 
					 warehouse_number_of_workers	INTEGER			NOT NULL,
					 insert_dt						TIMESTAMP 		NOT NULL,
					 update_dt						TIMESTAMP		NOT NULL
					);
				
				INSERT INTO bl_3nf.ce_warehouses
					(	
						warehouse_id,
						warehouse_src_id,
						source_system,
						source_table,
						address_id,
						warehouse_area,
						warehouse_number_of_workers,
						insert_dt,
						update_dt
					)
				VALUES (-1, 'N/A', 'MANUAL', 'MANUAL', -1, -1 , -1, '1.1.1900', '1.1.1900'); 
				
				CREATE TABLE IF NOT EXISTS bl_3nf.ce_employees
					(
						employee_id						INTEGER			NOT NULL PRIMARY KEY,
						employee_src_id					VARCHAR(50)		NOT NULL,
						source_system					VARCHAR(50)		NOT NULL,
						source_table					VARCHAR(50)		NOT NULL,
						employee_first_name				VARCHAR(255)	NOT NULL, 
						employee_last_name				VARCHAR(255)	NOT NULL, 
						employee_full_name				VARCHAR(255)	NOT NULL,
						employee_email					VARCHAR(255)	NOT NULL,
						employee_phone					VARCHAR(255)	NOT NULL,
						employee_salary					VARCHAR(255)	NOT NULL, 
						insert_dt						TIMESTAMP 		NOT NULL,
						update_dt						TIMESTAMP		NOT NULL 
					);
				
				INSERT INTO bl_3nf.ce_employees
					(	
						employee_id,
						employee_src_id,
						source_system,
						source_table,
						employee_first_name,
						employee_last_name,
						employee_full_name,
						employee_email,
						employee_phone,
						employee_salary,
						insert_dt,
						update_dt
					)
				VALUES (-1, 'N/A', 'MANUAL', 'MANUAL', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', '1.1.1900', '1.1.1900'); 
				
				CREATE TABLE IF NOT EXISTS bl_3nf.ce_promotions
					(
						promotion_id		INTEGER			NOT NULL PRIMARY KEY,
						promotion_src_id	VARCHAR(50)		NOT NULL,
						source_system		VARCHAR(50)		NOT NULL,
						source_table		VARCHAR(50)		NOT NULL,
						promotion_channel	VARCHAR(255)	NOT NULL,
						promotion_desc		VARCHAR(1500)	NOT NULL,
						insert_dt			TIMESTAMP 		NOT NULL,
						update_dt			TIMESTAMP 		NOT NULL
					);
				
				--Default row
				INSERT INTO bl_3nf.ce_promotions
					(	
						promotion_id,
						promotion_src_id,
						source_system,
					 	source_table,
					 	promotion_channel,
					 	promotion_desc,
					 	insert_dt,
					 	update_dt
					)
				VALUES (-1, 'N/A', 'MANUAL', 'MANUAL', 'N/A', 'N/A', '1.1.1900', '1.1.1900');
				
				CREATE TABLE IF NOT EXISTS bl_3nf.ce_deliveries
					(
						delivery_id			INTEGER			NOT NULL PRIMARY KEY,
						delivery_src_id		VARCHAR(50)		NOT NULL,
						source_system		VARCHAR(50)		NOT NULL,
						source_table		VARCHAR(50)		NOT NULL,
						delivery_type_name	VARCHAR(255)	NOT NULL,
						delivery_details	VARCHAR(500)	NOT NULL,
						insert_dt			TIMESTAMP 		NOT NULL,
						update_dt			TIMESTAMP 		NOT NULL 
					);
							
								
				INSERT INTO bl_3nf.ce_deliveries
					(	
						delivery_id,
						delivery_src_id,
						source_system,
						source_table,
						delivery_type_name,
						delivery_details,
						insert_dt,
						update_dt
					)
				VALUES (-1, 'N/A', 'MANUAL', 'MANUAL', 'N/A', 'N/A', '1.1.1900', '1.1.1900');
			
				CREATE TABLE IF NOT EXISTS bl_3nf.ce_categories
					(
						category_id			INTEGER			NOT NULL PRIMARY KEY,
						category_src_id		VARCHAR(50)		NOT NULL,
						source_system		VARCHAR(50)		NOT NULL,
						source_table		VARCHAR(50)		NOT NULL,
						category_name		VARCHAR(50)		NOT NULL, 
						insert_dt			TIMESTAMP 		NOT NULL,
						update_dt			TIMESTAMP 		NOT NULL 
					);
								
				INSERT INTO bl_3nf.ce_categories
					(	
						category_id,
						category_src_id,
						source_system,
						source_table,
						category_name,
						insert_dt,
						update_dt
					)
				VALUES (-1, 'N/A', 'MANUAL', 'MANUAL', 'N/A', '1.1.1900', '1.1.1900');
				
				CREATE TABLE IF NOT EXISTS bl_3nf.ce_payment_types
					(
						payment_type_id		INTEGER			NOT NULL PRIMARY KEY,
						payment_type_src_id	VARCHAR(20)		NOT NULL,
						source_system		VARCHAR(50)		NOT NULL,
						source_table		VARCHAR(50)		NOT NULL,
						payment_type		VARCHAR(20)		NOT NULL, 
						insert_dt			TIMESTAMP 		NOT NULL,
						update_dt			TIMESTAMP 		NOT NULL 
					);	
								
				INSERT INTO bl_3nf.ce_payment_types
					(	
						payment_type_id,
						payment_type_src_id,
						source_system,
						source_table,
						payment_type, 
						insert_dt,
						update_dt
					)
				VALUES (-1, 'N/A', 'MANUAL', 'MANUAL', 'N/A', '1.1.1900', '1.1.1900');
				
				CREATE TABLE IF NOT EXISTS bl_3nf.fct_sales
					(
						product_id			INTEGER			NOT NULL,
						warehouse_id		INTEGER			NOT NULL,
						customer_id			INTEGER			NOT NULL,
						employee_id			INTEGER			NOT NULL,
						promotion_id		INTEGER			NOT NULL,
						delivery_id			INTEGER			NOT NULL,
						payment_type_id		INTEGER			NOT NULL,
						event_date 			DATE			NOT NULL, 
						quantity			INTEGER			NOT NULL,
						actual_price		DECIMAL(10,2)	NOT NULL, 
						discount_price		DECIMAL(10,2)	NOT NULL, 
						insert_dt			TIMESTAMP 		NOT NULL
					);
END; $$; 