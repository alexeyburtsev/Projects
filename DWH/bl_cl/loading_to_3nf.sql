----------------------------------------------------------------------------------------
--									ce_regions											
----------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bl_cl.populate_ce_regions()
LANGUAGE plpgsql
AS $$
BEGIN 
	WITH insertion AS (
						SELECT DISTINCT COALESCE(region_id, 'N/A')		 region_id,
					       	   			'sa_bank_card'					 source_system,
					       	   			'src_sales_bank_card'			 source_table,
					       	   			COALESCE(region_name, 'N/A')	 region_name,
					       	   			CURRENT_TIMESTAMP				 insert_dt,
					       	   			CURRENT_TIMESTAMP				 update_dt
						FROM sa_bank_card.src_sales_bank_card
						
						UNION ALL
						
						SELECT DISTINCT COALESCE(region_id, 'N/A')		 region_id,
					       				'sa_cash'						 source_system,
					       				'src_sales_cash'				 source_table,
					       				COALESCE(region_name, 'N/A')	 region_name,
					       				CURRENT_TIMESTAMP				 insert_dt,
					       				CURRENT_TIMESTAMP				 update_dt
						FROM sa_cash.src_sales_cash)
	
	MERGE INTO bl_3nf.ce_regions reg
	USING insertion ON UPPER(reg.region_src_id) = UPPER(COALESCE(insertion.region_id, 'N/A')) AND 
					   reg.source_system = insertion.source_system AND 
					   reg.source_table = insertion.source_table
					   
	WHEN MATCHED AND UPPER(insertion.region_name) != UPPER(reg.region_name)
	THEN 
		UPDATE 
		SET region_name = insertion.region_name,
			update_dt = CURRENT_TIMESTAMP
					   
	WHEN NOT MATCHED 
	THEN 
		INSERT  
			( 	region_id,
			    region_src_id,
				source_system,
				source_table,
				region_name,
				insert_dt,
				update_dt )
				
		VALUES  
			(	NEXTVAL('bl_3nf.seq_ce_regions'),
			    insertion.region_id,
			    insertion.source_system,
			    insertion.source_table,
			    insertion.region_name,
			    insertion.insert_dt,
			    insertion.update_dt );
	 
	                   	   
	CALL bl_cl.insert_logs('ce_regions',
						   'Inserting data from bl_cl to 3nf',
              			   'Data is successfully inserted into ce_regions');
              	
	EXCEPTION 
	WHEN OTHERS THEN 
		CALL bl_cl.insert_logs('ce_regions',
							   'Inserting data from bl_cl to 3nf',
              			   	   'Data insertion into ce_regions is failed'); 			   
END;
$$;

----------------------------------------------------------------------------------------
--									ce_countries											
----------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bl_cl.populate_ce_countries()
LANGUAGE plpgsql
AS $$
BEGIN 
	WITH insertion AS (
						SELECT DISTINCT COALESCE(country_id, 'N/A')		 country_id,
								        'sa_bank_card'					 source_system,
								        'src_sales_bank_card'			 source_table,
								        COALESCE(country_name, 'N/A')	 country_name,
								        COALESCE(reg.region_id, -1)	 	 region_id,
								        CURRENT_TIMESTAMP				 insert_dt,
								        CURRENT_TIMESTAMP				 update_dt
						FROM sa_bank_card.src_sales_bank_card src
						LEFT OUTER JOIN bl_3nf.ce_regions reg ON src.region_id = reg.region_src_id 
											 AND LOWER(reg.source_system) = 'sa_bank_card'
						
						UNION ALL
						
						SELECT DISTINCT COALESCE(country_id, 'N/A')		 country_id,
								        'sa_cash'						 source_system,
								        'src_sales_cash'				 source_table,
								        COALESCE(country_name, 'N/A')	 country_name,
								        COALESCE(reg.region_id, -1)	 	 region_id,
								        CURRENT_TIMESTAMP				 insert_dt,
								        CURRENT_TIMESTAMP				 update_dt
						FROM sa_cash.src_sales_cash src
						LEFT OUTER JOIN bl_3nf.ce_regions reg ON src.region_id = reg.region_src_id 
											 AND LOWER(reg.source_system) = 'sa_cash')
						 
	MERGE INTO bl_3nf.ce_countries con
	USING insertion ON UPPER(con.country_src_id) = UPPER(COALESCE(insertion.country_id, 'N/A')) AND 
					   LOWER(con.source_system) = LOWER(insertion.source_system) AND 
					   con.source_table = insertion.source_table
					   
	WHEN MATCHED AND UPPER(insertion.country_name) != UPPER(con.country_name) THEN 
	UPDATE SET country_name = insertion.country_name,
			   update_dt = CURRENT_TIMESTAMP
					   
	WHEN NOT MATCHED THEN 
	INSERT  
			( 	country_id,
				country_src_id,
				source_system,
				source_table,
				country_name,
				region_id,
				insert_dt,
				update_dt )
				
	VALUES  
			(	NEXTVAL('bl_3nf.seq_ce_countries'),
			    insertion.country_id,
			    insertion.source_system,
			    insertion.source_table,
			    insertion.country_name,
			    insertion.region_id,
			    insertion.insert_dt,
			    insertion.update_dt );
         	   
	CALL bl_cl.insert_logs('ce_countries',
  						   'Inserting data from bl_cl to 3nf',
              			   'Data is successfully inserted into ce_countries');
              	
	EXCEPTION 
		WHEN OTHERS THEN 
			CALL bl_cl.insert_logs('ce_countries',
								   'Inserting data from bl_cl to 3nf',
              			   		   'Data insertion into ce_countries is failed');                    	   
	                   	 
END;
$$;

----------------------------------------------------------------------------------------
--									ce_cities											
----------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bl_cl.populate_ce_cities()
LANGUAGE plpgsql
AS $$
BEGIN 
	WITH insertion AS (
						SELECT DISTINCT COALESCE(city_id, 'N/A')		city_id,
								        'sa_bank_card'					source_system,
								        'src_sales_bank_card'			source_table,
								        COALESCE(con.country_id, -1)	country_id,
									    COALESCE(city_name, 'N/A')		city_name,
								        CURRENT_TIMESTAMP				insert_dt,
								        CURRENT_TIMESTAMP				update_dt
						FROM sa_bank_card.src_sales_bank_card src
						LEFT JOIN bl_3nf.ce_countries con ON src.country_id = con.country_src_id 
											 WHERE LOWER(con.source_system) = 'sa_bank_card'
											 AND LOWER(con.source_table) = 'src_sales_bank_card'
											 
						UNION ALL
						
						SELECT DISTINCT COALESCE(city_id, 'N/A')		city_id,
								        'sa_cash'						source_system,
								        'src_sales_cash'				source_table,
								        COALESCE(con.country_id, -1)	country_id,
									    COALESCE(city_name, 'N/A')		city_name,
								        CURRENT_TIMESTAMP				insert_dt,
								        CURRENT_TIMESTAMP				update_dt
						FROM sa_cash.src_sales_cash src
						LEFT JOIN bl_3nf.ce_countries con ON src.country_id = con.country_src_id  
											 WHERE LOWER(con.source_system) = 'sa_cash'
											 AND LOWER(con.source_table) = 'src_sales_cash')
											 
	MERGE INTO bl_3nf.ce_cities cit
	USING insertion ON UPPER(cit.city_src_id) = UPPER(COALESCE(insertion.city_id, 'N/A')) AND 
					   LOWER(cit.source_system) = LOWER(insertion.source_system) AND 
					   LOWER(cit.source_table) = LOWER(insertion.source_table)
					   
	WHEN MATCHED AND insertion.country_id != cit.country_id OR 
					 UPPER(insertion.city_name) != UPPER(cit.city_name) THEN 
	UPDATE SET country_id = insertion.country_id,
			   city_name = insertion.city_name,
			   update_dt = CURRENT_TIMESTAMP
					   
	WHEN NOT MATCHED THEN 
	INSERT  
			( 	city_id,
			    city_src_id,
			    source_system,
			    source_table,
			    country_id,
			    city_name,
				insert_dt,
				update_dt )
				
	VALUES  
			(	NEXTVAL('bl_3nf.seq_ce_cities'),
			    insertion.city_id,
			    insertion.source_system,
			    insertion.source_table,
			    insertion.country_id,
			    insertion.city_name,
			    insertion.insert_dt,
			    insertion.update_dt );
	                   	   
	CALL bl_cl.insert_logs('ce_cities',
						   'Inserting data from bl_cl to 3nf',
              			   'Data is successfully inserted into ce_cities');
              	
	EXCEPTION 
		WHEN OTHERS THEN 
			CALL bl_cl.insert_logs('ce_cities',
								   'Inserting data from bl_cl to 3nf',
              			   		   'Data insertion into ce_cities is failed');                    	   
END;
$$;

----------------------------------------------------------------------------------------
--									ce_addresses											
----------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bl_cl.populate_ce_addresses()
LANGUAGE plpgsql
AS $$
BEGIN 
	WITH insertion AS (
						SELECT DISTINCT COALESCE(address_id, 'N/A')		 address_id,
								        'sa_bank_card'					 source_system,
								        'src_sales_bank_card'			 source_table,
								        COALESCE(cit.city_id, -1)		 city_id,
									    COALESCE(address_name, 'N/A')	 address_name,
								        CURRENT_TIMESTAMP				 insert_dt,
								        CURRENT_TIMESTAMP				 update_dt
						FROM sa_bank_card.src_sales_bank_card src
						LEFT JOIN bl_3nf.ce_cities cit ON src.city_id = cit.city_src_id 
											 AND LOWER(cit.source_system) = 'sa_bank_card'
											 
						UNION ALL
						
						SELECT DISTINCT COALESCE(address_id, 'N/A')		 address_id,
								        'sa_cash'						 source_system,
								        'src_sales_cash'				 source_table,
								        COALESCE(cit.city_id, -1)		 city_id,
									    COALESCE(address_name, 'N/A')	 address_name,
								        CURRENT_TIMESTAMP				 insert_dt,
								        CURRENT_TIMESTAMP				 update_dt
						FROM sa_cash.src_sales_cash src
						LEFT JOIN bl_3nf.ce_cities cit ON src.city_id = cit.city_src_id 
											 AND LOWER(cit.source_system) = 'sa_cash')
	
	MERGE INTO bl_3nf.ce_addresses addr
	USING insertion ON UPPER(addr.address_src_id) = UPPER(COALESCE(insertion.address_id, 'N/A')) AND 
					   LOWER(addr.source_system) = LOWER(insertion.source_system) AND 
					   addr.source_table = insertion.source_table
					   
	WHEN MATCHED AND UPPER(insertion.address_name) != UPPER(addr.address) THEN 
	UPDATE SET address = insertion.address_name,
			   update_dt = CURRENT_TIMESTAMP
					   
	WHEN NOT MATCHED THEN 
	INSERT  
			( 	address_id,
			    address_src_id,
			    source_system,
			    source_table,
			    city_id,
			    address,
				insert_dt,
				update_dt )
				
	VALUES  
			(	NEXTVAL('bl_3nf.seq_ce_addresses'),
			    insertion.address_id,
			    insertion.source_system,
			    insertion.source_table,
			    insertion.city_id,
			    insertion.address_name,
			    insertion.insert_dt,
			    insertion.update_dt );					 
	                   	   
	CALL bl_cl.insert_logs('ce_addresses',
						   'Inserting data from bl_cl to 3nf',
              			   'Data is successfully inserted into ce_addresses');
              	
	EXCEPTION 
		WHEN OTHERS THEN 
			CALL bl_cl.insert_logs('ce_addresses',
								   'Inserting data from bl_cl to 3nf',
              			   		   'Data insertion into ce_addresses is failed');                    	   
END;
$$;

--------------------------------------------------------------------------------------------
--									ce_customers
--------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bl_cl.populate_ce_customers()
LANGUAGE plpgsql
AS $$
BEGIN 
	WITH insertion AS (
						SELECT DISTINCT COALESCE(customer_id, 'N/A')			customer_id,
								        'sa_bank_card'							source_system,
								        'src_sales_bank_card'					source_table,
								--        COALESCE(addr.address_id, -1)			address_id,
								        COALESCE(customer_first_name, 'N/A')	customer_first_name,
									    COALESCE(customer_last_name, 'N/A')		customer_last_name,
									    COALESCE(customer_full_name, 'N/A')		customer_full_name,
									    COALESCE(customer_email, 'N/A')			customer_email,
									    COALESCE(customer_phone, 'N/A')			customer_phone,
								        CURRENT_TIMESTAMP						insert_dt,
								        CURRENT_TIMESTAMP						update_dt
						FROM sa_bank_card.src_sales_bank_card src
						LEFT JOIN bl_3nf.ce_addresses addr ON src.address_id = addr.address_src_id 
											 AND LOWER(addr.source_system) = 'sa_bank_card'
											 
						UNION ALL
						
						SELECT DISTINCT COALESCE(customer_id, 'N/A')			customer_id,
								        'sa_cash'								source_system,
								        'src_sales_cash'						source_table,
								--        COALESCE(addr.address_id, -1)			address_id,
								        COALESCE(customer_first_name, 'N/A')	customer_first_name,
									    COALESCE(customer_last_name, 'N/A')		customer_last_name,
									    COALESCE(customer_full_name, 'N/A')		customer_full_name,
									    COALESCE(customer_email, 'N/A')			customer_email,
									    COALESCE(customer_phone, 'N/A')			customer_phone,
								        CURRENT_TIMESTAMP						insert_dt,
								        CURRENT_TIMESTAMP						update_dt
						FROM sa_cash.src_sales_cash src
						LEFT JOIN bl_3nf.ce_addresses addr ON src.address_id = addr.address_src_id  
											 AND LOWER(addr.source_system) = 'sa_cash')
	
	MERGE INTO bl_3nf.ce_customers cust
	USING insertion ON UPPER(cust.customer_src_id) = UPPER(COALESCE(insertion.customer_id, 'N/A')) AND 
					   LOWER(cust.source_system) = LOWER(insertion.source_system) AND 
					   cust.source_table = insertion.source_table
					   
	WHEN MATCHED AND UPPER(insertion.customer_full_name) != UPPER(cust.customer_full_name) THEN 
	UPDATE SET customer_full_name = insertion.customer_full_name,
			   update_dt = CURRENT_TIMESTAMP
					   
	WHEN NOT MATCHED THEN 
	INSERT  
			( 	customer_id,
			    customer_src_id,
			    source_system,
			    source_table,
			--    address_id,
			    customer_first_name,
			    customer_last_name,
			    customer_full_name,
			    customer_email,
			    customer_phone,
				insert_dt,
				update_dt )
				
	VALUES  
			(	NEXTVAL('bl_3nf.seq_ce_customers'),
			    insertion.customer_id,
 			    insertion.source_system,
			    insertion.source_table,
			--    insertion.address_id,
			    insertion.customer_first_name,
			    insertion.customer_last_name,
			    insertion.customer_full_name,
			    insertion.customer_email,
			    insertion.customer_phone,
			    insertion.insert_dt,
			    insertion.update_dt );											 
	                   	   
	CALL bl_cl.insert_logs('ce_customers',
						   'Inserting data from bl_cl to 3nf',
              			   'Data is successfully inserted into ce_customers');
              	
	EXCEPTION 
		WHEN OTHERS THEN 
			CALL bl_cl.insert_logs('ce_customers',
								   'Inserting data from bl_cl to 3nf',
              			   		   'Data insertion into ce_customers is failed');                     	       	   
END;
$$;

--------------------------------------------------------------------------------------------
--									ce_warehouses
-------------------------------------------------------------------------------------------- 
CREATE OR REPLACE PROCEDURE bl_cl.populate_ce_warehouses()
LANGUAGE plpgsql
AS $$
BEGIN 
	WITH insertion AS (
			SELECT DISTINCT COALESCE(warehouse_id, 'N/A')						warehouse_id,
		       				'sa_bank_card'									    source_system,
		       				'src_sales_bank_card'								source_table,
		       				COALESCE(addr.address_id, -1)						address_id,
						    COALESCE(warehouse_area::INTEGER, -1)				warehouse_area,
						    COALESCE(warehouse_number_of_workers::INTEGER, -1)	warehouse_number_of_workers,
					        CURRENT_TIMESTAMP									insert_dt,
					        CURRENT_TIMESTAMP									update_dt
			FROM sa_bank_card.src_sales_bank_card src
			LEFT JOIN bl_3nf.ce_addresses addr ON src.address_id = addr.address_src_id 
								 AND LOWER(addr.source_system) = 'sa_bank_card'
								 
			UNION ALL
			
			SELECT DISTINCT COALESCE(warehouse_id, 'N/A')				 		warehouse_id,
		       				'sa_cash'											source_system,
		       				'src_sales_cash'									source_table,
		       				COALESCE(addr.address_id, -1)						address_id,
						    COALESCE(warehouse_area::INTEGER, -1)				warehouse_area,
						    COALESCE(warehouse_number_of_workers::INTEGER, -1)	warehouse_number_of_workers,
					        CURRENT_TIMESTAMP									insert_dt,
					        CURRENT_TIMESTAMP									update_dt
			FROM sa_cash.src_sales_cash src
			LEFT JOIN bl_3nf.ce_addresses addr ON src.address_id = addr.address_src_id  
								 AND LOWER(addr.source_system) = 'sa_cash')
	
	MERGE INTO bl_3nf.ce_warehouses warh
	USING insertion ON UPPER(warh.warehouse_src_id) = UPPER(COALESCE(insertion.warehouse_id, 'N/A')) AND 
					   LOWER(warh.source_system) = LOWER(insertion.source_system) AND 
					   warh.source_table = insertion.source_table
					   
	WHEN MATCHED THEN 
	UPDATE SET warehouse_area = insertion.warehouse_area,
			   warehouse_number_of_workers = insertion.warehouse_number_of_workers,
			   update_dt = CURRENT_TIMESTAMP
					   
	WHEN NOT MATCHED THEN 
	INSERT  
			( 	warehouse_id,
			    warehouse_src_id,
			    source_system,
			    source_table,
			    address_id,
			    warehouse_area,
			    warehouse_number_of_workers,
				insert_dt,
				update_dt )
				
	VALUES  
			(	NEXTVAL('bl_3nf.seq_ce_warehouses'),
			    insertion.warehouse_id,
			    insertion.source_system,
			    insertion.source_table,
			    insertion.address_id,
			    insertion.warehouse_area,
			    insertion.warehouse_number_of_workers,
			    insertion.insert_dt,
			    insertion.update_dt );									  
	                   	   
	CALL bl_cl.insert_logs('ce_warehouses',
						   'Inserting data from bl_cl to 3nf',
              			   'Data is successfully inserted into ce_warehouses');
              	
	EXCEPTION 
		WHEN OTHERS THEN 
			CALL bl_cl.insert_logs('ce_warehouses',
								   'Inserting data from bl_cl to 3nf',
              			   		   'Data insertion into ce_warehouses is failed');                     	       	              	   
END;
$$;

--------------------------------------------------------------------------------------------
--									ce_employees
--------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bl_cl.populate_ce_employees()
LANGUAGE plpgsql
AS $$
BEGIN 
	WITH insertion AS (
						SELECT DISTINCT COALESCE(employee_id, 'N/A')				employee_id,
					       				'sa_bank_card'								source_system,
								        'src_sales_bank_card'						source_table,
								--        COALESCE(addr.address_id, -1)				address_id,
									    COALESCE(employee_first_name, 'N/A')		employee_first_name,
									    COALESCE(employee_last_name, 'N/A')			employee_last_name,
									    COALESCE(employee_full_name, 'N/A')			employee_full_name,
									    COALESCE(employee_email, 'N/A')				employee_email,
									    COALESCE(employee_phone, 'N/A')				employee_phone,
									    COALESCE(employee_salary_$, 'N/A')			employee_salary,
								        CURRENT_TIMESTAMP							insert_dt,
								        CURRENT_TIMESTAMP							update_dt
						FROM sa_bank_card.src_sales_bank_card src
						LEFT JOIN bl_3nf.ce_addresses addr ON src.address_id = addr.address_src_id 
											 AND LOWER(addr.source_system) = 'sa_bank_card'
											 
						UNION ALL
						
						SELECT DISTINCT COALESCE(employee_id, 'N/A')				employee_id,
					       				'sa_cash'									source_system,
								        'src_sales_cash'							source_table,
							--	        COALESCE(addr.address_id, -1)				address_id,
									    COALESCE(employee_first_name, 'N/A')		employee_first_name,
									    COALESCE(employee_last_name, 'N/A')			employee_last_name,
									    COALESCE(employee_full_name, 'N/A')			employee_full_name,
									    COALESCE(employee_email, 'N/A')				employee_email,
									    COALESCE(employee_phone, 'N/A')				employee_phone,
									    COALESCE(employee_salary_$, 'N/A')			employee_salary,
								        CURRENT_TIMESTAMP							insert_dt,
								        CURRENT_TIMESTAMP							update_dt
						FROM sa_cash.src_sales_cash src
						LEFT JOIN bl_3nf.ce_addresses addr ON src.address_id = addr.address_src_id  
											 AND LOWER(addr.source_system) = 'sa_cash')
											 
	MERGE INTO bl_3nf.ce_employees emp
	USING insertion ON UPPER(emp.employee_src_id) = UPPER(COALESCE(insertion.employee_id, 'N/A')) AND 
					   LOWER(emp.source_system) = LOWER(insertion.source_system) AND 
					   emp.source_table = insertion.source_table
					   
	WHEN MATCHED AND UPPER(insertion.employee_full_name) != UPPER(emp.employee_full_name) THEN 
	UPDATE SET employee_first_name = insertion.employee_first_name,
			   employee_last_name = insertion.employee_last_name,
			   employee_full_name = insertion.employee_full_name,
			   employee_email = insertion.employee_email,
			   update_dt = CURRENT_TIMESTAMP
					   
	WHEN NOT MATCHED THEN 
	INSERT  
			( 	employee_id,
			    employee_src_id,
			    source_system,
			    source_table,
			--    address_id,
			    employee_first_name,
			    employee_last_name,
			    employee_full_name,
			    employee_email,
			    employee_phone,
			    employee_salary,
				insert_dt,
				update_dt )
				
	VALUES  
			(	NEXTVAL('bl_3nf.seq_ce_employees'),
			    insertion.employee_id,
			    insertion.source_system,
			    insertion.source_table,
			--    insertion.address_id,
			    insertion.employee_first_name,
			    insertion.employee_last_name,
			    insertion.employee_full_name,
			    insertion.employee_email,
			    insertion.employee_phone,
			    insertion.employee_salary,
			    insertion.insert_dt,
			    insertion.update_dt );			
	                   	   
	CALL bl_cl.insert_logs('ce_employees',
						   'Inserting data from bl_cl to 3nf',
              			   'Data is successfully inserted into ce_employees');
              	
	EXCEPTION 
		WHEN OTHERS THEN 
			CALL bl_cl.insert_logs('ce_employees',
								   'Inserting data from bl_cl to 3nf',
              			   		   'Data insertion into ce_employees is failed');                      	              	   
END;
$$;

------------------------------------------------------------------------------------------
--									ce_promotions
------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bl_cl.populate_ce_promotions()
LANGUAGE plpgsql
AS $$
BEGIN 
	WITH insertion AS (
						SELECT DISTINCT COALESCE(promotion_id, 'N/A')				promotion_id,
						   				'sa_bank_card'								source_system,
						   				'src_sales_bank_card'						source_table,		
						   				COALESCE(promotion_channel, 'N/A')			promotion_channel,
						   				COALESCE(promotion_desc, 'N/A')				promotion_desc,
								        CURRENT_TIMESTAMP							insert_dt,
								        CURRENT_TIMESTAMP							update_dt
						FROM sa_bank_card.src_sales_bank_card 
						
						UNION ALL
						
						SELECT DISTINCT COALESCE(promotion_id, 'N/A')				promotion_id,
						   				'sa_cash'									source_system,
						   				'src_sales_cash'							source_table,		
						   				COALESCE(promotion_channel, 'N/A')			promotion_channel,
						   				COALESCE(promotion_desc, 'N/A')				promotion_desc,
								        CURRENT_TIMESTAMP							insert_dt,
								        CURRENT_TIMESTAMP							update_dt
						FROM sa_cash.src_sales_cash )
				
	MERGE INTO bl_3nf.ce_promotions prom
	USING insertion ON UPPER(prom.promotion_src_id) = UPPER(COALESCE(insertion.promotion_id, 'N/A')) AND 
					   LOWER(prom.source_system) = LOWER(insertion.source_system) AND 
					   prom.source_table = insertion.source_table
					   
	WHEN MATCHED AND UPPER(insertion.promotion_channel) != UPPER(prom.promotion_channel) THEN 
	UPDATE SET promotion_channel = insertion.promotion_channel,
			   promotion_desc = insertion.promotion_desc,
			   update_dt = CURRENT_TIMESTAMP
					   
	WHEN NOT MATCHED THEN 
	INSERT  
			( 	promotion_id,
 		 	    promotion_src_id,
			    source_system,
		 	    source_table,
		 	    promotion_channel,
		 	    promotion_desc,
				insert_dt,
				update_dt )
				
	VALUES  
			(	NEXTVAL('bl_3nf.seq_ce_promotions'),
			    insertion.promotion_id,
			    insertion.source_system,
		 	    insertion.source_table,
		 	    insertion.promotion_channel,
		 	    insertion.promotion_desc,
			    insertion.insert_dt,
			    insertion.update_dt );								
						
	CALL bl_cl.insert_logs('ce_promotions',
						   'Inserting data from bl_cl to 3nf',
              			   'Data is successfully inserted into ce_promotions');
              	
	EXCEPTION 
		WHEN OTHERS THEN 
			CALL bl_cl.insert_logs('ce_promotions',
								   'Inserting data from bl_cl to 3nf',
              			   		   'Data insertion into ce_promotions is failed');                    	   	   
END;
$$;

--------------------------------------------------------------------------------------------
--									ce_deliveries
--------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bl_cl.populate_ce_deliveries()
LANGUAGE plpgsql
AS $$
BEGIN 
	WITH insertion AS (
						SELECT DISTINCT COALESCE(delivery_id, 'N/A')				delivery_id,
					       				'sa_bank_card'								source_system,
					       				'src_sales_bank_card'						source_table,
					       				COALESCE(delivery_type_name, 'N/A')			delivery_type_name,
					       				COALESCE(delivery_details, 'N/A')			delivery_details,
								        CURRENT_TIMESTAMP							insert_dt,
								        CURRENT_TIMESTAMP							update_dt
						FROM sa_bank_card.src_sales_bank_card 
						
						UNION ALL
						
						SELECT DISTINCT COALESCE(delivery_id, 'N/A')				delivery_id,
						   				'sa_cash'									source_system,
						   				'src_sales_cash'							source_table,		
						   				COALESCE(delivery_type_name, 'N/A')			delivery_type_name,
					       				COALESCE(delivery_details, 'N/A')			delivery_details,
								        CURRENT_TIMESTAMP							insert_dt,
								        CURRENT_TIMESTAMP							update_dt
						FROM sa_cash.src_sales_cash )
						
	MERGE INTO bl_3nf.ce_deliveries del
	USING insertion ON UPPER(del.delivery_src_id) = UPPER(COALESCE(insertion.delivery_id, 'N/A')) AND 
					   LOWER(del.source_system) = LOWER(insertion.source_system) AND 
					   del.source_table = insertion.source_table
					   
	WHEN MATCHED AND UPPER(insertion.delivery_type_name) != UPPER(del.delivery_type_name) THEN 
	UPDATE SET delivery_type_name = insertion.delivery_type_name,
			   delivery_details = insertion.delivery_details,
			   update_dt = CURRENT_TIMESTAMP
					   
	WHEN NOT MATCHED THEN 
	INSERT  
			( 	delivery_id,
			    delivery_src_id,
			    source_system,
			    source_table,
			    delivery_type_name,
			    delivery_details,
				insert_dt,
				update_dt )
				
	VALUES  
			(	NEXTVAL('bl_3nf.seq_ce_deliveries'),
			    insertion.delivery_id,
			    insertion.source_system,
			    insertion.source_table,
			    insertion.delivery_type_name,
			    insertion.delivery_details,
			    insertion.insert_dt,
			    insertion.update_dt );	  
	                   	   
	CALL bl_cl.insert_logs('ce_deliveries',
						   'Inserting data from bl_cl to 3nf',
              			   'Data is successfully inserted into ce_deliveries');
              	
	EXCEPTION 
		WHEN OTHERS THEN 
			CALL bl_cl.insert_logs('ce_deliveries',
								   'Inserting data from bl_cl to 3nf',
              			   		   'Data insertion into ce_deliveries is failed');                    	    
END;
$$;

--------------------------------------------------------------------------------------------
--									ce_categories
--------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bl_cl.populate_ce_categories()
LANGUAGE plpgsql
AS $$
BEGIN 
	WITH insertion AS (
						SELECT DISTINCT COALESCE(category_id, 'N/A')		 category_id,
					       				'sa_bank_card'						 source_system,
					       				'src_sales_bank_card'				 source_table,
					       				COALESCE(category_name, 'N/A')		 category_name,
								        CURRENT_TIMESTAMP					 insert_dt,
								        CURRENT_TIMESTAMP					 update_dt
						FROM sa_bank_card.src_sales_bank_card 
						
						UNION ALL
						
						SELECT DISTINCT COALESCE(category_id, 'N/A')		 category_id,
					       				'sa_cash'							 source_system,
					       				'src_sales_cash'					 source_table,
					       				COALESCE(category_name, 'N/A')		 category_name,
								        CURRENT_TIMESTAMP					 insert_dt,
								        CURRENT_TIMESTAMP					 update_dt
						FROM sa_cash.src_sales_cash )
		
	MERGE INTO bl_3nf.ce_categories cat
	USING insertion ON UPPER(cat.category_src_id) = UPPER(COALESCE(insertion.category_id, 'N/A')) AND 
					   LOWER(cat.source_system) = LOWER(insertion.source_system) AND 
					   cat.source_table = insertion.source_table
					   
	WHEN MATCHED AND UPPER(insertion.category_name) != UPPER(cat.category_name) THEN 
	UPDATE SET category_name = insertion.category_name,
			   update_dt = CURRENT_TIMESTAMP
					   
	WHEN NOT MATCHED THEN 
	INSERT  
			( 	category_id,
			    category_src_id,
			    source_system,
			    source_table,
			    category_name,
				insert_dt,
				update_dt )
				
	VALUES  
			(	NEXTVAL('bl_3nf.seq_ce_categories'),
			    insertion.category_id,
		   		insertion.source_system,
				insertion.source_table,
		   		insertion.category_name,
			    insertion.insert_dt,
			    insertion.update_dt );	  					 
	                   	   
	CALL bl_cl.insert_logs('ce_categories',
						   'Inserting data from bl_cl to 3nf',
              			   'Data is successfully inserted into ce_categories');
              	
	EXCEPTION 
		WHEN OTHERS THEN 
			CALL bl_cl.insert_logs('ce_categories',
								   'Inserting data from bl_cl to 3nf',
              			   		   'Data insertion into ce_categories is failed');                            	   
END;
$$;		
--------------------------------------------------------------------------------------------
--									ce_payment_types
--------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bl_cl.populate_ce_payment_types()
LANGUAGE plpgsql
AS $$
BEGIN 
	WITH insertion AS (
						SELECT DISTINCT COALESCE(payment_type_id, 'N/A')	 payment_type_id,
					       				'sa_bank_card'						 source_system,
					       				'src_sales_bank_card'				 source_table,
					       				COALESCE(payment_type, 'N/A')		 payment_type,
								        CURRENT_TIMESTAMP					 insert_dt,
								        CURRENT_TIMESTAMP					 update_dt
						FROM sa_bank_card.src_sales_bank_card src LEFT JOIN bl_3nf.ce_categories cat
															   ON src.category_id = cat.category_src_id 
															   AND cat.source_system = 'sa_bank_card' 
						UNION ALL
						
						SELECT DISTINCT COALESCE(payment_type_id, 'N/A')	 payment_type_id,
					       				'sa_cash'							 source_system,
					       				'src_sales_cash'					 source_table,
					       				COALESCE(payment_type, 'N/A')		 payment_type,
								        CURRENT_TIMESTAMP					 insert_dt,
								        CURRENT_TIMESTAMP					 update_dt
						FROM sa_cash.src_sales_cash src LEFT JOIN bl_3nf.ce_categories cat
															   ON src.category_id = cat.category_src_id 
															   AND cat.source_system = 'sa_cash')
															   
	MERGE INTO bl_3nf.ce_payment_types pt
	USING insertion ON UPPER(pt.payment_type_src_id) = UPPER(COALESCE(insertion.payment_type_id, 'N/A')) AND 
					   LOWER(pt.source_system) = LOWER(insertion.source_system) AND 
					   pt.source_table = insertion.source_table
					   
	WHEN MATCHED AND UPPER(insertion.payment_type) != UPPER(pt.payment_type) THEN 
	UPDATE SET payment_type = insertion.payment_type,
			   update_dt = CURRENT_TIMESTAMP
					   
	WHEN NOT MATCHED THEN 
	INSERT  
			( 	payment_type_id,		
		  	    payment_type_src_id,	
		  	    source_system,	
		  	    source_table,	
		  	    payment_type,
				insert_dt,
				update_dt )
				
	VALUES  
			(	NEXTVAL('bl_3nf.seq_ce_payment_types'),
			    insertion.payment_type_id,			
		  	    insertion.source_system,	
		  	    insertion.source_table,	
		  	    insertion.payment_type,
			    insertion.insert_dt,
			    insertion.update_dt );	
															                	   
	CALL bl_cl.insert_logs('ce_payment_types',
						   'Inserting data from bl_cl to 3nf',
              			   'Data is successfully inserted into ce_payment_types');
              	
	EXCEPTION 
		WHEN OTHERS THEN 
			CALL bl_cl.insert_logs('ce_payment_types',
								   'Inserting data from bl_cl to 3nf',
              			   		   'Data insertion into ce_payment_types is failed');                     	             	   
END;
$$;

--------------------------------------------------------------------------------------------
--									ce_products
--------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bl_cl.populate_ce_products()
LANGUAGE plpgsql
AS $$
DECLARE 
	inserted_count INTEGER := 0;
	previous_load_date_src_1 timestamp;
	previous_load_date_src_2 timestamp;
BEGIN 
	
	UPDATE bl_3nf.ce_products pr
	SET is_active = FALSE,
		end_dt = CURRENT_TIMESTAMP  
	FROM sa_bank_card.src_sales_bank_card src 
	WHERE 
		pr.product_src_id = src.product_id AND 
		pr.product_name <> src.product_name AND 
		source_system = 'sa_bank_card' AND 
		pr.start_dt <> CURRENT_TIMESTAMP ;
	
	UPDATE bl_3nf.ce_products pr
	SET is_active = FALSE,
		end_dt = CURRENT_TIMESTAMP 
	FROM sa_cash.src_sales_cash src 
	WHERE 
		pr.product_src_id = src.product_id AND 
		pr.product_name <> src.product_name AND 
		source_system = 'sa_cash' AND 
		pr.start_dt <> CURRENT_TIMESTAMP ;
---------	
	SELECT inc.previous_load_date
	FROM bl_cl.prm_mta_incremental_load AS inc
	WHERE inc.source_table_name = 'src_sales_bank_card' AND target_table_name = 'ce_products'
	INTO previous_load_date_src_1;

	INSERT INTO bl_3nf.ce_products 
	SELECT NEXTVAL('bl_3nf.seq_ce_products'),
		   COALESCE(src.product_id, 'N/A'), 
		   'sa_bank_card',
		   'src_sales_bank_card', 
		   COALESCE(cat.category_id, -1),
		   COALESCE(src.product_name, 'N/A'), 
		   COALESCE(src.is_active, 'FALSE')::bool, 
		   CURRENT_TIMESTAMP,  				
		  '9999-12-31'::TIMESTAMP                          
	FROM (SELECT DISTINCT src.product_id, src.product_name, src.is_active
		  FROM sa_bank_card.src_sales_bank_card src) s
	LEFT JOIN bl_3nf.ce_categories cat ON src.category_id = cat.category_src_id 
									  AND cat.source_system = 'sa_bank_card'
	LEFT JOIN bl_3nf.ce_products pr ON src.product_id = pr.product_src_id 
									  AND pr.source_system = 'sa_bank_card'
									  AND pr.product_name = src.product_name 
	WHERE pr.product_id IS NULL AND s.insert_dt > previous_load_date_src_1;
		
	CALL bl_cl.incremental_load('sa_bank_card', 'ce_products', 'populate_ce_products', current_timestamp::timestamp);
	
----------
	SELECT inc.previous_load_date
	FROM bl_cl.prm_mta_incremental_load AS inc
	WHERE inc.source_table_name = 'src_sales_cash' AND target_table_name = 'ce_products'
	INTO previous_load_date_src_2;
	
	INSERT INTO bl_3nf.ce_products 
	SELECT NEXTVAL('bl_3nf.seq_ce_products'),
		   COALESCE(src.product_id, 'N/A'), 
		   'sa_bank_card',
		   'src_sales_bank_card', 
		   COALESCE(cat.category_id, -1),
		   COALESCE(src.product_name, 'N/A'), 
		   COALESCE(src.is_active, 'FALSE')::bool, 
		   CURRENT_TIMESTAMP,  				
		  '9999-12-31'::TIMESTAMP                          
	FROM (SELECT DISTINCT src.product_id, src.product_name, src.is_active
		  FROM sa_bank_card.src_sales_bank_card src) s
	LEFT JOIN bl_3nf.ce_categories cat ON src.category_id = cat.category_src_id 
									  AND cat.source_system = 'sa_cash'
	LEFT JOIN bl_3nf.ce_products pr ON src.product_id = pr.product_src_id 
									  AND pr.source_system = 'sa_cash'
									  AND pr.product_name = src.product_name 
	WHERE pr.product_id IS NULL AND s.insert_dt > previous_load_date_src_1;
		
	CALL bl_cl.incremental_load('sa_cash', 'ce_products', 'populate_ce_products', current_timestamp::timestamp);

	CALL bl_cl.insert_logs('ce_products',
						   'Inserting data from bl_cl to 3nf',
              			   'Data is successfully inserted into ce_products');
       			  
	EXCEPTION 
		WHEN OTHERS THEN 
			CALL bl_cl.insert_logs('ce_products',
								   'Inserting data from bl_cl to 3nf',
              			   		   'Data insertion into ce_products is failed');                    	                     	    
END; $$;

--------------------------------------------------------------------------------------------
--									fct_sales
--------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bl_cl.populate_fct_sales()
LANGUAGE plpgsql
AS $$
BEGIN 
	
		CREATE OR REPLACE VIEW bl_cl.incr_sales AS (
		WITH sales_bc AS (SELECT DISTINCT COALESCE(p.product_id, -1)						product_id,
								        COALESCE(w.warehouse_id, -1)						warehouse_id,
								        COALESCE(c.customer_id, -1)							customer_id,
								        COALESCE(e.employee_id, -1)							employee_id,
								        COALESCE(pr.promotion_id, -1)						promotion_id,
								        COALESCE(d.delivery_id, -1)							delivery_id, 
								        COALESCE(pt.payment_type_id, -1)					payment_type_id,
								        event_date::date								    event_date,
								        COALESCE(bc.quantity::INTEGER, -1) 					quantity,
								        COALESCE(bc.actual_price::DECIMAL(10, 2), -1)		actual_price,
								        COALESCE(bc.discount_price::DECIMAL(10, 2), -1)     discount_price,
									    CURRENT_TIMESTAMP									insert_dt
					    FROM sa_bank_card.src_sales_bank_card bc
						LEFT JOIN bl_3nf.ce_products p 		ON bc.product_id = p.product_src_id  	 		 
															AND p.source_system  = 'sa_bank_card'
															AND p.source_table = 'src_sales_bank_card'
						LEFT JOIN bl_3nf.ce_warehouses w 		ON bc.warehouse_id = w.warehouse_src_id  		 
															AND w.source_system  = 'sa_bank_card'
															AND w.source_table = 'src_sales_bank_card'
						LEFT JOIN bl_3nf.ce_customers c 		ON bc.customer_id = c.customer_src_id 	 		 
															AND c.source_system  = 'sa_bank_card'
															AND c.source_table = 'src_sales_bank_card'
						LEFT JOIN bl_3nf.ce_employees e 		ON bc.employee_id = e.employee_src_id 	 		 
															AND e.source_system  = 'sa_bank_card'
															AND e.source_table = 'src_sales_bank_card'
						LEFT JOIN bl_3nf.ce_promotions pr 		ON bc.promotion_id = pr.promotion_src_id 		 
															AND pr.source_system = 'sa_bank_card'
															AND pr.source_table = 'src_sales_bank_card'
						LEFT JOIN bl_3nf.ce_deliveries d 		ON bc.delivery_id = d.delivery_src_id	 		 
															AND d.source_system  = 'sa_bank_card'
															AND d.source_table = 'src_sales_bank_card'
						LEFT JOIN bl_3nf.ce_payment_types pt 	ON bc.payment_type_id = pt.payment_type_src_id	 
															AND pt.source_system  = 'sa_bank_card'
															AND pt.source_table = 'src_sales_bank_card'),
						
			 sales_csh AS ( SELECT DISTINCT COALESCE(p.product_id, -1)							product_id,
									        COALESCE(w.warehouse_id, -1)						warehouse_id,
									        COALESCE(c.customer_id, -1)							customer_id,
									        COALESCE(e.employee_id, -1)							employee_id,
									        COALESCE(pr.promotion_id, -1)						promotion_id,
									        COALESCE(d.delivery_id, -1)							delivery_id, 
									        COALESCE(pt.payment_type_id, -1)					payment_type_id,
									        event_date::date									event_date,
									        COALESCE(csh.quantity::INTEGER, -1) 				quantity,
									        COALESCE(csh.actual_price::DECIMAL(10, 2), -1)		actual_price,
									        COALESCE(csh.discount_price::DECIMAL(10, 2), -1)    discount_price,
										    CURRENT_TIMESTAMP									insert_dt
						    FROM sa_cash.src_sales_cash csh
							LEFT JOIN bl_3nf.ce_products p 			ON csh.product_id = p.product_src_id  	 		 
																		AND p.source_system  = 'sa_cash'
																		AND p.source_table = 'src_sales_cash'
							LEFT JOIN bl_3nf.ce_warehouses w 		ON csh.warehouse_id = w.warehouse_src_id  		 
																		AND w.source_system  = 'sa_cash'
																		AND w.source_table = 'src_sales_cash'
							LEFT JOIN bl_3nf.ce_customers c 		ON csh.customer_id = c.customer_src_id 	 		 
																		AND c.source_system  = 'sa_cash'
																		AND c.source_table = 'src_sales_cash'
							LEFT JOIN bl_3nf.ce_employees e 		ON csh.employee_id = e.employee_src_id 	 		 
																		AND e.source_system  = 'sa_cash'
																		AND e.source_table = 'src_sales_cash'
							LEFT JOIN bl_3nf.ce_promotions pr 		ON csh.promotion_id = pr.promotion_src_id 		 
																		AND pr.source_system = 'sa_cash'
																		AND pr.source_table = 'src_sales_cash'
							LEFT JOIN bl_3nf.ce_deliveries d 		ON csh.delivery_id = d.delivery_src_id	 		 
																		AND d.source_system  = 'sa_cash'
																		AND d.source_table = 'src_sales_cash'
							LEFT JOIN bl_3nf.ce_payment_types pt 	ON csh.payment_type_id = pt.payment_type_src_id	 
																		AND pt.source_system  = 'sa_cash'
																		AND pt.source_table = 'src_sales_cash')
						SELECT * FROM sales_bc									
						WHERE sales_bc.insert_dt > (SELECT previous_load_date 
						   				   FROM bl_cl.prm_mta_incremental_load   
						   				   WHERE source_table_name='src_sales_bank_card')
						UNION ALL 												
																		
						SELECT * FROM sales_csh								
						WHERE sales_csh.insert_dt > (SELECT previous_load_date 
						   				   FROM bl_cl.prm_mta_incremental_load   
						   				   WHERE source_table_name='src_sales_cash'));


			INSERT INTO bl_3nf.fct_sales 
			SELECT * FROM bl_cl.incr_sales; 

			UPDATE bl_cl.prm_mta_incremental_load 
			SET previous_load_date = CURRENT_TIMESTAMP 
			WHERE source_table_name='src_sales_bank_card' OR source_table_name='src_sales_cash';
		
CALL bl_cl.insert_logs('fct_sales',
					   'Inserting data from bl_cl to 3nf',
              		   'Data is successfully inserted into fct_sales');
              	
EXCEPTION 
	WHEN OTHERS THEN 
		CALL bl_cl.insert_logs('fct_sales',
							   'Inserting data from bl_cl to 3nf',
           			   		   'Data insertion into fct_sales is failed');                    	                     	   
END;
$$;

CALL bl_cl.populate_fct_sales();

--------------------------------------------------------------------------------------------
--									POPULATE_ALL
--------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION load_data_to_3nf()
RETURNS SETOF bl_cl.logging 
AS $$
DECLARE procedures_to_load_3nf TEXT[] := ARRAY[
	'bl_cl.populate_ce_regions',
	'bl_cl.populate_ce_countries',
	'bl_cl.populate_ce_cities',
	'bl_cl.populate_ce_addresses',
	'bl_cl.populate_ce_customers',
	'bl_cl.populate_ce_warehouses',
	'bl_cl.populate_ce_employees',
	'bl_cl.populate_ce_promotions',
	'bl_cl.populate_ce_deliveries',
	'bl_cl.populate_ce_categories',
	'bl_cl.populate_ce_payment_types',
	'bl_cl.populate_ce_products',
	'bl_cl.populate_fct_sales'
];
BEGIN 
	FOR i IN 1 .. ARRAY_LENGTH(procedures_to_load_3nf, 1) 
	LOOP  
		EXECUTE 'CALL ' || procedures_to_load_3nf[i] || '();';
	END LOOP;

	RETURN QUERY
		SELECT *
		FROM bl_cl.logging; 
END;
$$ LANGUAGE plpgsql;

SELECT * FROM load_data_to_3nf();
