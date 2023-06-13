CREATE TYPE bl_cl.duplicates_table AS (
    table_ TEXT,
    duplicate_count integer
);

CREATE OR REPLACE FUNCTION bl_cl.duplicates_test()
RETURNS SETOF bl_cl.duplicates_table
LANGUAGE plpgsql
AS $$
DECLARE
	schema_ TEXT := 'bl_3nf';
	table_ TEXT;
	src_id_col TEXT;
	duplicate_count integer := 0;
BEGIN 
	
	FOR table_, src_id_col, schema_ IN (
		SELECT table_name, column_name, table_schema
		FROM information_schema.columns
		WHERE table_schema = 'bl_3nf' AND column_name LIKE '%src_id')
	LOOP
		EXECUTE format ('SELECT count(*)
				          FROM (
				              SELECT count(*)
				              FROM %I.%I
				              GROUP BY %I, source_system, source_table
				              HAVING COUNT(*) > 1
				          ) AS duplicate_rows', schema_, table_, src_id_col)
				         INTO duplicate_count;
				        
		RETURN NEXT (table_, duplicate_count);
		        
    END LOOP;
    
END; $$;

-- SELECT * FROM bl_cl.duplicates_test();

----------------------------2_TEST-----------------------------------
CREATE OR REPLACE PROCEDURE bl_cl.test_for_missing_rows()
LANGUAGE plpgsql
AS $$
DECLARE 
		source_sales_number integer := 0;
		bl_3nf_sales_number integer := 0;
		bl_dm_sales_number integer := 0;
		missing_rows_number_3nf integer := 0;
		missing_rows_number_dm integer := 0;
BEGIN 

SELECT COUNT(*)	
INTO source_sales_number 
FROM ( SELECT DISTINCT product_id,
					   warehouse_id,
					   customer_id,
					   employee_id,
					   promotion_id,
					   delivery_id,
					   payment_type_id,
					   event_date
		 FROM sa_bank_card.src_sales_bank_card src_1
		 UNION ALL
		 SELECT DISTINCT product_id,
						 warehouse_id,
						 customer_id,
						 employee_id,
						 promotion_id,
						 delivery_id,
						 payment_type_id,
						 event_date
		 FROM sa_cash.src_sales_cash src_2 ) a ;
			
SELECT COUNT(*)
INTO bl_3nf_sales_number
FROM (SELECT DISTINCT product_id,
					  warehouse_id,
					  customer_id,
					  employee_id,
					  promotion_id,
					  delivery_id,
					  payment_type_id,
					  event_date
	  FROM bl_3nf.fct_sales fs2) b ;	
	
SELECT source_sales_number - bl_3nf_sales_number
INTO missing_rows_number_3nf;	
	
IF missing_rows_number_3nf = 0 THEN 
		RAISE NOTICE 'All records from SA layer are represented in 3NF layer.';
	ELSIF missing_rows_number_3nf > 0 THEN 
		RAISE NOTICE 'Some records records from SA layer are not presented in 3NF layer.';
	ELSE 
		RAISE NOTICE 'There are more records in 3NF layer than in SA due to data quality.';
END IF; 

SELECT COUNT(*)
INTO bl_dm_sales_number
FROM (SELECT DISTINCT product_surr_id,
					  warehouse_surr_id,
					  customer_surr_id,
					  employee_surr_id,
					  promotion_surr_id,
					  delivery_surr_id,
					  payment_type_surr_id,
					  sale_dt
	  FROM bl_dm.fct_sales fs2) c ;	
	 
SELECT source_sales_number - bl_dm_sales_number
INTO missing_rows_number_dm;	

IF missing_rows_number_dm = 0 THEN 
		RAISE NOTICE 'All records from SA layer are represented in DM layer.';
ELSIF missing_rows_number_dm > 0 THEN 
		RAISE NOTICE 'Some records from SA layer are not presented in DM layer.';
ELSE 
		RAISE NOTICE 'There are more records in DM layer than in SA due to data quality.';
	
END IF; 
END; $$; 


--CALL bl_cl.test_for_missing_rows();