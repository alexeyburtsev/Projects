----------------------------------------------------------------------------------------
--									dim_employees											
----------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE bl_cl.populate_dim_employees()

LANGUAGE plpgsql
AS $$
BEGIN 
	
	MERGE INTO bl_dm.dim_employees emp
	USING (
			SELECT 
				employee_id,
				'bl_3nf' source_system,
				'bl_3nf.ce_employees' source_table,
				employee_first_name, 
				employee_last_name, 
				employee_full_name,
				employee_email,
				employee_phone,
				employee_salary, 
				insert_dt,
				update_dt
			FROM bl_3nf.ce_employees
			WHERE employee_id != -1
			) src ON emp.employee_id::int = src.employee_id
				 AND emp.source_system = src.source_system
				 AND emp.source_table = src.source_table
					   
	WHEN MATCHED THEN 
		 UPDATE SET employee_first_name = src.employee_first_name,
		 			employee_last_name = src.employee_last_name,
		 			employee_full_name = src.employee_full_name,
		 			employee_email = src.employee_email,
		 			employee_phone = src.employee_phone,
		 			employee_salary = src.employee_salary
					   
	WHEN NOT MATCHED THEN 
		 INSERT  
			( 	employee_surr_id,
				employee_id,
				source_system,
				source_table,
				employee_first_name,
				employee_last_name,
				employee_full_name,
				employee_email,
				employee_phone,
				employee_salary,
				insert_dt,
				update_dt)
				
		VALUES  
			(	NEXTVAL('bl_dm.seq_dim_employees'),
				employee_id,
				source_system,
				source_table,
				employee_first_name,
				employee_last_name,
				employee_full_name,
				employee_email,
				employee_phone,
				employee_salary,
				CURRENT_TIMESTAMP,
				CURRENT_TIMESTAMP);
	 
	                   	   
	CALL bl_cl.insert_logs('dim_employees',
						   'Inserting data from 3nf to dim',
              			   'Data is successfully inserted into dim_employees');
              	
	EXCEPTION 
	WHEN OTHERS THEN 
		CALL bl_cl.insert_logs('dim_employees',
							   'Inserting data from 3nf to dim',
              			   	   'Data insertion into dim_employees is failed'); 			   
END;
$$;

--SELECT count(*) FROM bl_dm.dim_employees;
--------------------------------------------------------------------------------------------
--									dim_customers
--------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bl_cl.populate_dim_customers()

LANGUAGE plpgsql
AS $$
BEGIN 
	
	MERGE INTO bl_dm.dim_customers cust
	USING (
			SELECT 
				customer_id,
				'bl_3nf' source_system,
				'bl_3nf.ce_customers' source_table,
				customer_first_name,
				customer_last_name,
				customer_full_name,
				customer_email,
				customer_phone,
				insert_dt,
				update_dt
			FROM bl_3nf.ce_customers 
			WHERE customer_id != -1
			) src ON cust.customer_id::int = src.customer_id
				 AND cust.source_system = src.source_system
				 AND cust.source_table = src.source_table
					   
	WHEN MATCHED THEN 
		 UPDATE SET 
		 			customer_first_name = src.customer_first_name, 
					customer_last_name = src.customer_last_name, 
					customer_full_name = src.customer_full_name,
					customer_email = src.customer_email,
					customer_phone = src.customer_phone
					   
	WHEN NOT MATCHED THEN 
		 INSERT  
			( 	customer_surr_id,
				customer_id,
				source_system,
				source_table,
				customer_first_name, 
				customer_last_name, 
				customer_full_name,
				customer_email,
				customer_phone,
				insert_dt,
				update_dt)
				
		VALUES  
			(	NEXTVAL('bl_dm.seq_dim_customers'),
				customer_id,
				source_system,
				source_table,
				customer_first_name, 
				customer_last_name, 
				customer_full_name,
				customer_email,
				customer_phone,
				CURRENT_TIMESTAMP,
				CURRENT_TIMESTAMP);
	 
	                   	   
	CALL bl_cl.insert_logs('dim_customers',
						   'Inserting data from 3nf to dim',
              			   'Data is successfully inserted into dim_customers');
              	
	EXCEPTION 
	WHEN OTHERS THEN 
		CALL bl_cl.insert_logs('dim_customers',
							   'Inserting data from 3nf to dim',
              			   	   'Data insertion into dim_customers is failed'); 			   
END;
$$;

--------------------------------------------------------------------------------------------
--									dim_deliveries
--------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bl_cl.populate_dim_deliveries()

LANGUAGE plpgsql
AS $$
BEGIN 

	MERGE INTO bl_dm.dim_deliveries del
	USING (
			SELECT 
				delivery_id,
				'bl_3nf' source_system,
				'bl_3nf.ce_deliveries' source_table,
				delivery_type_name,
				delivery_details,
				insert_dt,
				update_dt
			FROM bl_3nf.ce_deliveries 
			WHERE delivery_id != -1
			) src ON del.delivery_id::int = src.delivery_id
				 AND del.source_system = src.source_system
				 AND del.source_table = src.source_table
					   
	WHEN MATCHED THEN 
		 UPDATE SET delivery_type_name = src.delivery_type_name,
		 			delivery_details = src.delivery_details
					   
	WHEN NOT MATCHED THEN 
		 INSERT  
			( 	delivery_surr_id,
				delivery_id,
				source_system,
				source_table,
				delivery_type_name,
				delivery_details,
				insert_dt,
				update_dt)
				
		VALUES  
			(	NEXTVAL('bl_dm.seq_dim_deliveries'),
				delivery_id,
				source_system,
				source_table,
				delivery_type_name,
				delivery_details,
				CURRENT_TIMESTAMP,
				CURRENT_TIMESTAMP);
	 
	                   	   
	CALL bl_cl.insert_logs('dim_deliveries',
						   'Inserting data from 3nf to dim',
              			   'Data is successfully inserted into dim_deliveries');
              	
	EXCEPTION 
	WHEN OTHERS THEN 
		CALL bl_cl.insert_logs('dim_deliveries',
							   'Inserting data from 3nf to dim',
              			   	   'Data insertion into dim_deliveries is failed'); 			   
END;
$$;
 
--------------------------------------------------------------------------------------------
--									dim_promotions
--------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bl_cl.populate_dim_promotions()

LANGUAGE plpgsql
AS $$
BEGIN 
	
	MERGE INTO bl_dm.dim_promotions prom
	USING (
			SELECT 
				promotion_id,
				source_system,
				source_table,
				promotion_channel,
				promotion_desc,
				insert_dt,
				update_dt
			FROM bl_3nf.ce_promotions 
			WHERE promotion_id != -1
			) src ON prom.promotion_id::int = src.promotion_id
				 AND prom.source_system = src.source_system
				 AND prom.source_table = src.source_table
					   
	WHEN MATCHED THEN 
		 UPDATE SET promotion_channel = src.promotion_channel,
		 			promotion_desc = src.promotion_desc
					   
	WHEN NOT MATCHED THEN 
		 INSERT  
			( 	promotion_surr_id,
				promotion_id,
				source_system,
				source_table,
				promotion_channel,
				promotion_desc,
				insert_dt,
				update_dt)
				
		VALUES  
			(	NEXTVAL('bl_dm.seq_dim_promotions'),
				promotion_id,
				source_system,
				source_table,
				promotion_channel,
				promotion_desc,
				CURRENT_TIMESTAMP,
				CURRENT_TIMESTAMP);
	 
	                   	   
	CALL bl_cl.insert_logs('dim_promotions',
						   'Inserting data from 3nf to dim',
              			   'Data is successfully inserted into dim_promotions');
              	
	EXCEPTION 
	WHEN OTHERS THEN 
		CALL bl_cl.insert_logs('dim_promotions',
							   'Inserting data from 3nf to dim',
              			   	   'Data insertion into dim_promotions is failed'); 			   
END;
$$;

--------------------------------------------------------------------------------------------
--									dim_products_scd
--------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bl_cl.populate_dim_products()

LANGUAGE plpgsql
AS $$
BEGIN 
	
	MERGE INTO bl_dm.dim_products prod
	USING (
			SELECT 
				product_id,
				'bl_3nf' AS source_system,
				'ce_products' AS source_table,
				product_name,
				pr.category_id,
				cat.category_name,
				is_active,
				start_dt,
				end_dt
			FROM bl_3nf.ce_products pr
			LEFT JOIN bl_3nf.ce_categories cat ON pr.category_id = cat.category_id 
			WHERE product_id != -1
			) src ON prod.product_id::int = src.product_id
				 AND prod.source_system = src.source_system
				 AND prod.source_table = src.source_table
					   
	WHEN MATCHED THEN 
		 UPDATE SET product_name = src.product_name,
					product_category = src.category_name,
					is_active = src.is_active
					
	WHEN NOT MATCHED THEN 
		 INSERT  
			( 	product_surr_id,
				product_id,
				source_system,
				source_table,
				product_name,
				product_category_id,
				product_category,
				is_active,
				start_dt,
				end_dt)
				
		VALUES  
			(	NEXTVAL('bl_dm.seq_dim_products'),
				product_id,
				source_system,
				source_table,
				product_name,
				category_id,
				category_name,
				is_active,
				CURRENT_TIMESTAMP,
				CURRENT_TIMESTAMP);
	 
	                   	   
	CALL bl_cl.insert_logs('dim_products',
						   'Inserting data from 3nf to dim',
              			   'Data is successfully inserted into dim_products');
              	
	EXCEPTION 
	WHEN OTHERS THEN 
		CALL bl_cl.insert_logs('dim_products',
							   'Inserting data from 3nf to dim',
              			   	   'Data insertion into dim_products is failed'); 			   
END;
$$;

--------------------------------------------------------------------------------------------
--									dim_warehouses
--------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bl_cl.populate_dim_warehouses()

LANGUAGE plpgsql
AS $$
BEGIN 
	
	MERGE INTO bl_dm.dim_warehouses warh
	USING (SELECT 
				warehouse_id,
				'bl_3nf' AS source_system,
				'ce_warehouses' AS source_table,
				cit.city_id AS warehouse_city_id,
				cit.city_name AS warehouse_city_name, 
				reg.region_id AS warehouse_region_id,
				reg.region_name AS warehouse_region_name,
				con.country_id AS warehouse_country_id,
				con.country_name AS warehouse_country_name,
				addr.address_id AS address_id,
				warehouse_area, 
				warehouse_number_of_workers,
				CURRENT_TIMESTAMP,
				CURRENT_TIMESTAMP					
			FROM bl_3nf.ce_warehouses w 
			LEFT JOIN bl_3nf.ce_addresses addr ON w.address_id = addr.address_id 
			LEFT JOIN bl_3nf.ce_cities cit ON addr.city_id = cit.city_id 
			LEFT JOIN bl_3nf.ce_countries con ON cit.country_id = con.country_id 
			LEFT JOIN bl_3nf.ce_regions reg ON con.region_id = reg.region_id 
			WHERE warehouse_id != -1
			) src ON warh.warehouse_id::int = src.warehouse_id
				 AND warh.source_system = src.source_system
				 AND warh.source_table = src.source_table
					   
	WHEN MATCHED THEN 
		 UPDATE SET warehouse_area = src.warehouse_area,
		 			warehouse_number_of_workers = src.warehouse_number_of_workers
					
	WHEN NOT MATCHED THEN 
		 INSERT  
			( 	warehouse_surr_id,
			    warehouse_id, 
			    source_system,
			    source_table,
			    warehouse_city_id,
			    warehouse_city_name, 
			    warehouse_region_id,
			    warehouse_region_name,
			    warehouse_country_id, 
			    warehouse_country_name,
			    address_id, 
			    warehouse_area,
			    warehouse_number_of_workers, 		
			    insert_dt,
			    update_dt)
				
		VALUES  
			(	NEXTVAL('bl_dm.seq_dim_warehouses'),
				warehouse_id,
				source_system,
				source_table,
				warehouse_city_id,
				warehouse_city_name,
				warehouse_region_id,
				warehouse_region_name,
				warehouse_country_id,
				warehouse_country_name,
				address_id,
				warehouse_area, 
				warehouse_number_of_workers,
				CURRENT_TIMESTAMP,
				CURRENT_TIMESTAMP);
	 
	                   	   
	CALL bl_cl.insert_logs('dim_warehouses',
						   'Inserting data from 3nf to dim',
              			   'Data is successfully inserted into dim_warehouses');
              	
	EXCEPTION 
	WHEN OTHERS THEN 
		CALL bl_cl.insert_logs('dim_warehouses',
							   'Inserting data from 3nf to dim',
              			   	   'Data insertion into dim_warehouses is failed'); 			   
END;
$$;

--------------------------------------------------------------------------------------------
--									dim_payment_types
--------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bl_cl.populate_dim_payment_types()

LANGUAGE plpgsql
AS $$
BEGIN 
	
	MERGE INTO bl_dm.dim_payment_types pt
	USING (
			SELECT 
				payment_type_id,
				source_system,
				source_table,
				payment_type,
				insert_dt,
				update_dt					
			FROM bl_3nf.ce_payment_types 
			WHERE payment_type_id != -1
			) src ON pt.payment_type_id::int = src.payment_type_id
				 AND pt.source_system = src.source_system
				 AND pt.source_table = src.source_table
					   
	WHEN MATCHED THEN 
		 UPDATE SET payment_type = src.payment_type
					
	WHEN NOT MATCHED THEN 
		 INSERT  
			( 	payment_type_surr_id,
				payment_type_id,
				source_system,
				source_table,
				payment_type,
				insert_dt,
				update_dt)
				
		VALUES  
			(	NEXTVAL('bl_dm.seq_dim_payment_types'),
				payment_type_id,
				source_system,
				source_table,
				payment_type,
				CURRENT_TIMESTAMP,
				CURRENT_TIMESTAMP);
	 
	                   	   
	CALL bl_cl.insert_logs('dim_payment_types',
						   'Inserting data from 3nf to dim',
              			   'Data is successfully inserted into dim_payment_types');
              	
	EXCEPTION 
	WHEN OTHERS THEN 
		CALL bl_cl.insert_logs('dim_payment_types',
							   'Inserting data from 3nf to dim',
              			   	   'Data insertion into dim_payment_types is failed'); 			    
END;
$$;

--------------------------------------------------------------------------------------------
--									dim_dates
--------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bl_cl.populate_dim_dates()

LANGUAGE plpgsql
AS $$
BEGIN 
INSERT INTO bl_dm.dim_dates
		( --date_id,
		  full_date,
		  day_name,
		  day_of_month,
		  day_of_week,
		  day_of_quarter,
		  day_of_year,
		  week_of_month,
		  week_of_year,
		  month_number,
		  month_name,
		  month_name_abbr,
		  quarter_number,
		  quarter_name,
		  year_number,
		  first_day_of_week,
		  last_day_of_week,
		  first_day_of_month,
		  last_day_of_month,
		  first_day_of_quarter,
		  last_day_of_quarter,
		  first_day_of_year,
		  last_day_of_year,
		  mmyyyy,
		  mmddyyyy 	)

SELECT   --TO_CHAR(fulldate, 'yyyymmdd')::INT AS date_id,
		 fulldate AS full_date,
         TO_CHAR(fulldate, 'Day') AS day_name,
         EXTRACT(isodow FROM fulldate) AS day_of_week,
         EXTRACT(day FROM fulldate) AS day_of_month,
         fulldate - DATE_TRUNC('QUARTER', fulldate)::date + 1 AS day_of_quarter,
         EXTRACT(doy FROM fulldate) AS day_of_year,
         TO_CHAR(fulldate, 'W')::int AS week_of_month,
         EXTRACT(week FROM fulldate) AS week_of_year,
         EXTRACT(month FROM fulldate) AS month_number,
         TO_CHAR(fulldate, 'Month') AS month_name,
         TO_CHAR(fulldate, 'Mon') AS month_name_abbr,
         EXTRACT(QUARTER FROM fulldate) AS quarter_number,
         CASE
                  WHEN EXTRACT(QUARTER FROM fulldate) = 1 THEN 'First'
                  WHEN EXTRACT(QUARTER FROM fulldate) = 2 THEN 'Second'
                  WHEN EXTRACT(QUARTER FROM fulldate) = 3 THEN 'Third'
                  WHEN EXTRACT(QUARTER FROM fulldate) = 4 THEN 'Fourth'
         END AS quarter_name,
         EXTRACT(year FROM fulldate) AS year_number,
         fulldate + (1 - EXTRACT(isodow FROM fulldate)):: int  AS first_day_of_week,
         fulldate + (7 - EXTRACT(isodow FROM fulldate)):: int  AS last_day_of_week,
         fulldate + (1 - EXTRACT(DAY FROM fulldate)):: int  AS first_day_of_month,
         (DATE_TRUNC('MONTH', fulldate) + interval '1 MONTH - 1 day'):: date AS last_day_of_month, 
         DATE_TRUNC('QUARTER', fulldate):: date AS first_day_of_quarter,
         (DATE_TRUNC('QUARTER', fulldate) + interval '3 MONTH - 1 day')::date AS last_day_of_quarter,
         TO_DATE(EXTRACT(year FROM fulldate) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
         TO_DATE(EXTRACT(year FROM fulldate) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
         TO_CHAR(fulldate, 'mmyyyy') AS mmyyyy,
         TO_CHAR(fulldate, 'mmddyyyy') AS mmdd
FROM (
	      SELECT   '2003-01-01'::DATE + SEQUENCE.DAY AS fulldate
	      FROM     GENERATE_SERIES(0, 14610) AS SEQUENCE (DAY)
	      GROUP BY SEQUENCE.DAY
	 ) dates
ORDER BY fulldate
ON CONFLICT DO NOTHING;

CALL bl_cl.insert_logs('dim_dates',
					   'Inserting data from 3nf to dim',
              		   'Data is successfully inserted into dim_dates');
              	
	EXCEPTION 
	WHEN OTHERS THEN 
		CALL bl_cl.insert_logs('dim_dates',
							   'Inserting data from 3nf to dim',
              			   	   'Data insertion into dim_dates is failed');

END;
$$;

--------------------------------------------------------------------------------------------
--									fct_sales
--------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bl_cl.populate_fct_sales()

LANGUAGE plpgsql
AS $$
DECLARE 
	table_name TEXT := 'fct_sales';
	rows_affected int := 0;
	rows_count int := 0;
	start_time timestamp := now();
	end_time timestamp := now();
	error_message TEXT := 'no_error';
	error_context TEXT := 'no_error';
	last_max_event_date timestamp;
	partition_table TEXT;
BEGIN 
	

	SELECT COALESCE(to_date(max(date_id)::TEXT, 'YYYYMMDD'), '1900-01-01'::timestamp) FROM bl_dm.fct_sales
	INTO last_max_event_date;
	
	CALL bl_cl.create_partitions();
	
	INSERT INTO bl_dm.fct_sales
	SELECT COALESCE(delivery_surr_id, -1),
		   COALESCE(product_surr_id, -1),
		   COALESCE(customer_surr_id, -1),
		   COALESCE(employee_surr_id, -1),
		   COALESCE(warehouse_surr_id, -1),
		   COALESCE(promotion_surr_id, -1),
		   COALESCE(payment_type_surr_id, -1),
		   event_date AS sale_dt,
		   quantity,
		   actual_price, 
		   discount_price,
		   CURRENT_TIMESTAMP insert_dt	
	FROM bl_3nf.fct_sales sl
	LEFT JOIN bl_dm.dim_deliveries 	   dl ON sl.delivery_id = dl.delivery_id::int
	LEFT JOIN bl_dm.dim_products 		p ON sl.product_id = p.product_id::int
	LEFT JOIN bl_dm.dim_customers 		c ON sl.customer_id = c.customer_id::int
	LEFT JOIN bl_dm.dim_employees 		e ON sl.employee_id = e.employee_id::int
	LEFT JOIN bl_dm.dim_warehouses 		w ON sl.warehouse_id = w.warehouse_id::int
	LEFT JOIN bl_dm.dim_promotions 	   pr ON sl.promotion_id = pr.promotion_id::int
	LEFT JOIN bl_dm.dim_payment_types  pt ON sl.payment_type_id = pt.payment_type_id::int
	LEFT JOIN bl_dm.dim_dates			d ON sl.event_date = d.full_date
	WHERE sl.event_date::timestamp > last_max_event_date;

   
	CALL bl_cl.insert_logs('fct_sales',
						   'Inserting data from 3nf to dim',
              			   'Data is successfully inserted into fct_sales');
              	
	EXCEPTION 
	WHEN OTHERS THEN 
		CALL bl_cl.insert_logs('fct_sales',
							   'Inserting data from 3nf to dim',
              			   	   'Data insertion into fct_sales is failed'); 			   
END;
$$;

--------------------------------------------------------------------------------------------
--									fct_sales
--------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION load_data_to_dm_layer()
RETURNS SETOF bl_cl.logging 
AS $$
DECLARE procedures_to_load_dm TEXT[] := ARRAY[
	'bl_cl.populate_dim_employees',
	'bl_cl.populate_dim_customers',
	'bl_cl.populate_dim_deliveries',
	'bl_cl.populate_dim_promotions',
	'bl_cl.populate_dim_products',
	'bl_cl.populate_dim_warehouses',
	'bl_cl.populate_dim_payment_types',
	'bl_cl.populate_dim_dates',
	'bl_cl.populate_fct_sales'
];
BEGIN 
	FOR i IN 1 .. ARRAY_LENGTH(procedures_to_load_dm, 1) 
	LOOP  
		EXECUTE 'CALL ' || procedures_to_load_dm[i] || '();';
	END LOOP;

	RETURN QUERY
		SELECT *
		FROM bl_cl.logging; 
END;
$$ LANGUAGE plpgsql;

SELECT * FROM load_data_to_dm_layer(); 