CREATE OR REPLACE PROCEDURE bl_cl.load_sa_cash()
LANGUAGE plpgsql
AS $$
BEGIN 

		CREATE SCHEMA IF NOT EXISTS sa_cash;
		CREATE FOREIGN TABLE IF NOT EXISTS sa_cash.ext_sales_cash (
			 id VARCHAR(4000),
			 product_name VARCHAR(4000),
			 subcategory VARCHAR(4000),
			 ratings VARCHAR(4000),
			 no_of_ratings VARCHAR(4000),
			 payment_type VARCHAR(4000),
			 subcategory_id VARCHAR(4000),
			 quantity VARCHAR(4000),
			 warehouse_area VARCHAR(4000),
			 warehouse_number_of_workers VARCHAR(4000),
			 delivery_id VARCHAR(4000),
			 delivery_type_name VARCHAR(4000),
			 delivery_details VARCHAR(4000),
			 customer_first_name VARCHAR(4000),
			 customer_last_name VARCHAR(4000),
			 customer_email VARCHAR(4000),
			 customer_phone VARCHAR(4000),
			 customer_full_name VARCHAR(4000),
			 customer_id VARCHAR(4000),
			 employee_first_name VARCHAR(4000),
			 employee_last_name VARCHAR(4000),
			 employee_email VARCHAR(4000),
			 employee_phone VARCHAR(4000),
			 employee_salary_$ VARCHAR(4000),
			 employee_full_name VARCHAR(4000),
			 employee_id VARCHAR(4000),
			 promotion_channel VARCHAR(4000),
			 promotion_desc VARCHAR(4000),
			 promotion_id VARCHAR(4000),
			 country_name VARCHAR(4000),
			 address_name VARCHAR(4000),
			 region_name VARCHAR(4000),
			 address_id VARCHAR(4000),
			 country_id VARCHAR(4000),
			 region_id VARCHAR(4000),
			 actual_price VARCHAR(4000),
			 discount_price VARCHAR(4000),
			 product_id VARCHAR(4000),
			 payment_type_id VARCHAR(4000),
			 category_name VARCHAR(4000),
			 category_id VARCHAR(4000),
			 city_name VARCHAR(4000),
			 city_id VARCHAR(4000),
			 employee_address VARCHAR(4000),
			 customer_address VARCHAR(4000),
			 warehouse_id VARCHAR(4000),
			 warehouse_address VARCHAR(4000),
			 event_date VARCHAR(4000)
		)  SERVER import
		   OPTIONS (filename 'C:/Users/final_dataset/cut_cash_dataset.csv', 
		            format 'csv',
		            HEADER 'true',
		            ENCODING 'UTF8'
		);
		
		CREATE TABLE IF NOT EXISTS sa_cash.src_sales_cash (
		     id VARCHAR(4000),
			 product_name VARCHAR(4000),
			 subcategory VARCHAR(4000),
			 ratings VARCHAR(4000),
			 no_of_ratings VARCHAR(4000),
			 payment_type VARCHAR(4000),
			 subcategory_id VARCHAR(4000),
			 quantity VARCHAR(4000),
			 warehouse_area VARCHAR(4000),
			 warehouse_number_of_workers VARCHAR(4000),
			 delivery_id VARCHAR(4000),
			 delivery_type_name VARCHAR(4000),
			 delivery_details VARCHAR(4000),
			 customer_first_name VARCHAR(4000),
			 customer_last_name VARCHAR(4000),
			 customer_email VARCHAR(4000),
			 customer_phone VARCHAR(4000),
			 customer_full_name VARCHAR(4000),
			 customer_id VARCHAR(4000),
			 employee_first_name VARCHAR(4000),
			 employee_last_name VARCHAR(4000),
			 employee_email VARCHAR(4000),
			 employee_phone VARCHAR(4000),
			 employee_salary_$ VARCHAR(4000),
			 employee_full_name VARCHAR(4000),
			 employee_id VARCHAR(4000),
			 promotion_channel VARCHAR(4000),
			 promotion_desc VARCHAR(4000),
			 promotion_id VARCHAR(4000),
			 country_name VARCHAR(4000),
			 address_name VARCHAR(4000),
			 region_name VARCHAR(4000),
			 address_id VARCHAR(4000),
			 country_id VARCHAR(4000),
			 region_id VARCHAR(4000),
			 actual_price VARCHAR(4000),
			 discount_price VARCHAR(4000),
			 product_id VARCHAR(4000),
			 payment_type_id VARCHAR(4000),
			 category_name VARCHAR(4000),
			 category_id VARCHAR(4000),
			 city_name VARCHAR(4000),
			 city_id VARCHAR(4000),
			 employee_address VARCHAR(4000),
			 customer_address VARCHAR(4000),
			 warehouse_id VARCHAR(4000),
			 warehouse_address VARCHAR(4000),
			 event_date VARCHAR(4000)
		);
		
		INSERT INTO sa_cash.src_sales_cash (
			 id,
			 product_name,
			 subcategory,
			 ratings,
			 no_of_ratings,
			 payment_type,
			 subcategory_id,
			 quantity,
			 warehouse_area,
			 warehouse_number_of_workers,
			 delivery_id,
			 delivery_type_name,
			 delivery_details,
			 customer_first_name,
			 customer_last_name,
			 customer_email,
			 customer_phone,
			 customer_full_name,
			 customer_id,
			 employee_first_name,
			 employee_last_name,
			 employee_email,
			 employee_phone,
			 employee_salary_$,
			 employee_full_name,
			 employee_id,
			 promotion_channel,
			 promotion_desc,
			 promotion_id,
			 country_name,
			 address_name,
			 region_name,
			 address_id,
			 country_id,
			 region_id,
			 actual_price,
			 discount_price,
			 product_id,
			 payment_type_id,
			 category_name,
			 category_id,
			 city_name,
			 city_id,
			 employee_address,
			 customer_address,
			 warehouse_id,
			 warehouse_address,
			 event_date )
		SELECT 
			 id,
			 product_name,
			 subcategory,
			 ratings,
			 no_of_ratings,
			 payment_type,
			 subcategory_id,
			 quantity,
			 warehouse_area,
			 warehouse_number_of_workers,
			 delivery_id,
			 delivery_type_name,
			 delivery_details,
			 customer_first_name,
			 customer_last_name,
			 customer_email,
			 customer_phone,
			 customer_full_name,
			 customer_id,
			 employee_first_name,
			 employee_last_name,
			 employee_email,
			 employee_phone,
			 employee_salary_$,
			 employee_full_name,
			 employee_id,
			 promotion_channel,
			 promotion_desc,
			 promotion_id,
			 country_name,
			 address_name,
			 region_name,
			 address_id,
			 country_id,
			 region_id,
			 actual_price,
			 discount_price,
			 product_id,
			 payment_type_id,
			 category_name,
			 category_id,
			 city_name,
			 city_id,
			 employee_address,
			 customer_address,
			 warehouse_id,
			 warehouse_address,
			 event_date 
		FROM sa_cash.ext_sales_cash;
		
END; $$;	