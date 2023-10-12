CREATE TEMP TABLE bonuses AS 		
SELECT wp.id,
	   wp."name" workplace_name,
	   c."name" country_name,
	   transaction_date,
	   MIN(created_when::date) first_tip_date,	--transaction_date
	   LEAST(MIN(created_when::date), onboarding_date) bonus_start_date,
	   total_amount
FROM work_place wp LEFT JOIN
	 (SELECT workplace_id,
			 created_when::date  	 transaction_date,
			 amount 				 total_amount 
	  FROM transactions_history	
	  WHERE "type" = 'INCOMING'
	  GROUP BY DATE_TRUNC('day', created_when),
			   workplace_id,
			   created_when,
			   amount) th ON wp.id = th.workplace_id 
			   	   LEFT JOIN country c ON wp.country_id = c.id 
GROUP BY wp.id,
	   	 wp."name",
	   	 c."name",
	     transaction_date,
		 total_amount;
		
--SELECT * FROM bonuses;	
CREATE TEMP TABLE metrics AS 		
SELECT id,
	   workplace_name,
	   country_name,
	   first_tip_date,
	   bonus_start_date,
	   tips_for_21_days,
	   CASE WHEN country_name = 'United Arab Emirates' AND tips_for_21_days > 700 THEN 1
	   		WHEN country_name = 'United Arab Emirates' AND tips_for_21_days < 700 THEN 0
	   		WHEN country_name = 'United Kingdom'       AND tips_for_21_days > 80  THEN 1
	   		WHEN country_name = 'United Kingdom'       AND tips_for_21_days < 80  THEN 0
	   END location_bonus,
	   tips_0_30_days,
	   tips_31_60_days,
	   tips_61_90_days,
	   is_matured_bonus,
	   CASE WHEN country_name = 'United Arab Emirates' AND tips_0_30_days > 1000
	   												   AND tips_31_60_days > 1000
	   												   AND tips_61_90_days > 1000
	   												   THEN SUM(tips_0_30_days + tips_31_60_days + tips_61_90_days)
	   		WHEN country_name = 'United Kingdom' 	   AND tips_0_30_days > 225
	   												   AND tips_31_60_days > 225
	   												   AND tips_61_90_days > 225
	   												   THEN SUM(tips_0_30_days + tips_31_60_days + tips_61_90_days)
	   END GMV_bonus_base,
	   CASE WHEN country_name = 'United Arab Emirates' AND tips_0_30_days > 1000
	   												   AND tips_31_60_days > 1000
	   												   AND tips_61_90_days > 1000
	   												   THEN SUM(tips_0_30_days + tips_31_60_days + tips_61_90_days) * 0.15
	   		WHEN country_name = 'United Kingdom' 	   AND tips_0_30_days > 225
	   												   AND tips_31_60_days > 225
	   												   AND tips_61_90_days > 225
	   												   THEN SUM(tips_0_30_days + tips_31_60_days + tips_61_90_days) * 0.2
	   END GMV_bonus
FROM 	   
		(SELECT id,
			   workplace_name,
			   country_name,
			   first_tip_date,
			   bonus_start_date,
			   SUM(CASE 
				   WHEN DATE_PART('day', transaction_date::timestamp - bonus_start_date::timestamp) <= '21' 
				   THEN total_amount ELSE 0 END) tips_for_21_days,
			   SUM(CASE 
				   WHEN DATE_PART('day', transaction_date::timestamp - bonus_start_date::timestamp) <= '31' 
				   THEN total_amount ELSE 0 END) tips_0_30_days,
			   SUM(CASE 
			   	   WHEN DATE_PART('day', transaction_date::timestamp - (bonus_start_date + 31)::timestamp) <= '31'
			   	   THEN total_amount ELSE 0 END) tips_31_60_days,
			   SUM(CASE 
			   	   WHEN DATE_PART('day', transaction_date::timestamp - (bonus_start_date + 61)::timestamp) <= '31'
			   	   THEN total_amount ELSE 0 END) tips_61_90_days,
			   CASE 
				   WHEN CURRENT_DATE - bonus_start_date >= 90 
			   	   THEN TRUE ELSE FALSE END	is_matured_bonus
		FROM bonuses
		GROUP BY id,
			     workplace_name,
			     country_name,
			     first_tip_date,
			     bonus_start_date
		ORDER BY id) b
GROUP BY id,
	     workplace_name,
	     country_name,
	     first_tip_date,
	     bonus_start_date,
	     tips_for_21_days,
	     tips_0_30_days,
		 tips_31_60_days,
		 tips_61_90_days,
		 is_matured_bonus
ORDER BY id;

