SET search_path TO sh;
--*******************************************************************************************************************
--											Task 1.
--*******************************************************************************************************************
/*Build the query to generate a report about the most significant customers (which have maximum sales) through various 
sales channels. The 5 largest customers are required for each channel.
Column sales_percentage shows percentage of customer’s sales within channel sales*/
--*******************************************************************************************************************
WITH all_customers AS (SELECT c2.channel_desc,
						  	  c.cust_last_name,
						   	  c.cust_first_name,
						   	  TO_CHAR(SUM(s.amount_sold), '9,999,999,999') AS amount_sold,
						   	  RANK() OVER (PARTITION BY c2.channel_desc ORDER BY  c2.channel_desc, SUM(s.amount_sold) DESC) AS rank_number,
						   	  CONCAT(ROUND((SUM(s.amount_sold)  /  (SUM(SUM(amount_sold)) 
						   	  					OVER (PARTITION BY c2.channel_desc))) * 100, 5), '%') AS sales_percentage
					   FROM customers c INNER JOIN sales s 	ON c.cust_id = s.cust_id 
									 	INNER JOIN channels c2 ON s.channel_id = c2.channel_id
					   GROUP BY c2.channel_desc,
							 c.cust_id ,
						  	 c.cust_last_name,
						   	 c.cust_first_name)
						 
SELECT channel_desc,
	   cust_last_name,
	   cust_first_name,
	   amount_sold,
	   sales_percentage
FROM all_customers
WHERE rank_number <= 5;

--*******************************************************************************************************************
--													Task 2.
--*******************************************************************************************************************
/*Compose query to retrieve data for report with sales totals for all products in Photo category in Asia (use data for 
 2000 year). Calculate report total (YEAR_SUM).*/
--*******************************************************************************************************************
SELECT prod_name,
	   COALESCE(TO_CHAR(q1, '999999999D99'), ' ') AS q1,
	   COALESCE(TO_CHAR(q2, '999999999D99'), ' ') AS q2,
	   COALESCE(TO_CHAR(q3, '999999999D99'), ' ') AS q3,
	   COALESCE(TO_CHAR(q4, '999999999D99'), ' ') AS q4,
	   TO_CHAR(COALESCE(q1, 0) + COALESCE(q2, 0) + COALESCE(q3, 0) + COALESCE(q4, 0) , '999999999D99') AS year_sum
	 
FROM crosstab($$SELECT p.prod_name,
					   t.calendar_quarter_number,
					   SUM(s.amount_sold)
				FROM sales s LEFT JOIN products p   ON s.prod_id = p.prod_id
							 LEFT JOIN customers c  ON s.cust_id = c.cust_id
							 LEFT JOIN countries c2 ON c.country_id = c2.country_id
							 LEFT JOIN times t 	    ON s.time_id = t.time_id
				WHERE UPPER(p.prod_category) = 'PHOTO' AND UPPER(c2.country_region) = 'ASIA' AND EXTRACT(YEAR FROM s.time_id) = 2000
				GROUP BY s.prod_id,
						 t.calendar_quarter_number,
						 p.prod_name 
				ORDER BY p.prod_name  ASC,
						 t.calendar_quarter_number; $$,
				
				$$SELECT x FROM generate_series(1,4) x;$$	)
				
AS ct (prod_name TEXT, q1 DECIMAL(10,2), q2 DECIMAL(10,2), q3 DECIMAL(10,2), q4 DECIMAL(10,2));

--*******************************************************************************************************************
--													TASK 3
--*******************************************************************************************************************
/*
 	Build the query to generate a report about customers who were included into TOP 300 (based on the amount of sales) in 
 	1998, 1999 and 2001. This report should separate clients by sales channels, and, at the same time, channels should be 
 	calculated independently (i.e. only purchases made on selected channel are relevant).
*/
--*******************************************************************************************************************

SELECT channel_desc,
	   cust_id,
	   cust_last_name,
	   cust_first_name,
	   SUM(amount_sold)
FROM (
		SELECT c.channel_id,
			   c.channel_desc,
			   c2.cust_id,
			   c2.cust_last_name,
			   c2.cust_first_name,
			   SUM(s.amount_sold) AS amount_sold,
			   EXTRACT(YEAR FROM s.time_id),
			   ROW_NUMBER() OVER (PARTITION BY c.channel_id, EXTRACT (YEAR FROM s.time_id) ORDER BY SUM(s.amount_sold) DESC) AS rnmbr 
		FROM sales s INNER JOIN channels c   ON s.channel_id = c.channel_id
					 INNER JOIN customers c2 ON s.cust_id = c2.cust_id 
		WHERE EXTRACT (YEAR FROM s.time_id) IN (1998, 1999, 2001)
		GROUP BY EXTRACT (YEAR FROM s.time_id),
				 c.channel_id,
				 c.channel_desc,
			   	 c2.cust_id,
			     c2.cust_last_name,
			     c2.cust_first_name
	 ) AS tab
WHERE rnmbr <= 300
GROUP BY channel_id,
		 channel_desc,
		 cust_id,
		 cust_last_name,
		 cust_first_name
HAVING COUNT(*) = 3
ORDER BY SUM(amount_sold) DESC,
		 cust_id ASC;
			   
--*******************************************************************************************************************
--													TASK 4
--*******************************************************************************************************************
/*
	Build the query to generate the report about sales in America and Europe:
	Conditions:
		• TIMES.CALENDAR_MONTH_DESC: 2000-01, 2000-02, 2000-03
		• COUNTRIES.COUNTRY_REGION: Europe, Americas.
*/
--*******************************************************************************************************************

WITH country_sales AS (SELECT t.calendar_month_desc,
							  p.prod_category,
							  TO_CHAR(SUM(s.amount_sold) FILTER (WHERE UPPER(c2.country_region) = 'AMERICAS') 
							  							 OVER (PARTITION BY t.calendar_month_desc, p.prod_category), '999,999,999') AS Americas,
							  TO_CHAR(SUM(s.amount_sold) FILTER (WHERE UPPER(c2.country_region) = 'EUROPE') 	
							  							 OVER (PARTITION BY t.calendar_month_desc, p.prod_category), '999,999,999') AS Europe
				       FROM sales s INNER JOIN products p   ON s.prod_id = p.prod_id
				       				INNER JOIN customers c  ON s.cust_id = c.cust_id
				       				INNER JOIN countries c2 ON c.country_id = c2.country_id
				       				INNER JOIN times t 		ON s.time_id = t.time_id
				       WHERE UPPER(c2.country_region) IN ('EUROPE', 'AMERICAS') AND t.calendar_month_desc IN ('2000-01', '2000-02', '2000-03'))

SELECT calendar_month_desc,
	   prod_category,
	   Americas AS "American Sales",
	   Europe AS "Europe Sales"
FROM country_sales
GROUP BY calendar_month_desc,
		 prod_category,
		 Americas,
		 Europe
ORDER BY calendar_month_desc ASC,
	   	 prod_category ASC;
-------------------------------------------------------------------------

SELECT * 
FROM crosstab(
$$
WITH prepared_data AS 
			(
			 SELECT t.calendar_month_desc,
					p.prod_category,
					c2.country_region,
					ROUND(SUM(s.amount_sold)) AS amount_sold
			 FROM sales s LEFT JOIN products p   ON s.prod_id = p.prod_id 
			 			  LEFT JOIN times t	     ON t.time_id = s.time_id 
			 			  LEFT JOIN customers c  ON c.cust_id = s.cust_id 
			 			  LEFT JOIN countries c2 ON c2.country_id = c.country_id 
			 WHERE t.calendar_month_desc IN ('2000-01', '2000-02', '2000-03') AND  
			 		UPPER(c2.country_region) IN ('AMERICAS', 'EUROPE')
			 GROUP BY t.calendar_month_desc,
			 		  p.prod_category,
			 		  c2.country_region 
			 )
SELECT calendar_month_desc, 
	   prod_category,
	   country_region,
	   amount_sold
FROM prepared_data
$$,

$$
SELECT 'Americas'
UNION 
SELECT 'Europe'
$$)
AS tb (month_prod_category VARCHAR, prod_category VARCHAR, "Americas SALES" NUMERIC, "Europe SALES" NUMERIC);





