CREATE TEMP TABLE locations_desc AS 
SELECT w.work_place_id,
	   w."EasyTip ID",
	   w."Staff status",
	   w.tax_reporting_type,
	   w.payment_system,
	   w."KYC status",
	   wp.onboarding_date,
	   wp.name
FROM work_place wp LEFT JOIN  
			(SELECT  staff.work_place_id,
			         staff.staff_slot_id AS "EasyTip ID",
					 staff.status AS "Staff status",
					 staff.tax_reporting_type, 
					 au.payment_system, 
		 			 (
			         CASE
			             WHEN au.payment_system = 'STRIPE'
		                      THEN
			                   (SELECT stripe.status
			                    FROM stripe_account stripe
			                    WHERE stripe.id = au.stripe_account_id)
		              	 ELSE
			                   (SELECT CASE WHEN card.status = 'ACTIVE' THEN 'COMPLETE' ELSE NULL END
			                    FROM card
			                    WHERE card.user_id = au.id
			                      AND card.status IN ('ACTIVE')
			                    LIMIT 1) END
			          )	AS "KYC status"
			  FROM waiter_info staff LEFT JOIN app_user au ON staff.user_id = au.id
			  WHERE staff.status = 'ACTIVE'  	
			  ) w ON wp.id = w.work_place_id;

SELECT work_place_id, 
	   name,
	   SUM (CASE WHEN "KYC status" = 'COMPLETE' THEN 1 ELSE 0 END) * 100 / COUNT(DISTINCT "EasyTip ID") kyc_complete_perc,
	   SUM (CASE WHEN tax_reporting_type = 'SELF_REPORTING' THEN 1 ELSE 0 END) * 100 / COUNT(DISTINCT "EasyTip ID") self_reporting_perc,
	   SUM (CASE WHEN payment_system LIKE 'CHECKOUT%' THEN 1 ELSE 0 END) * 100 / COUNT(DISTINCT "EasyTip ID") checkout_perc,
	   COUNT(DISTINCT "EasyTip ID") number_of_employees,
	   (CASE 
	    WHEN DATE_PART('year', CURRENT_DATE)  = DATE_PART('year', onboarding_date) AND 
	    	 DATE_PART('month', CURRENT_DATE) = DATE_PART('month', onboarding_date)
	    	 THEN 0
	    ELSE (DATE_PART('year', CURRENT_DATE)  - DATE_PART('year', onboarding_date)) * 12 +
             (DATE_PART('month', CURRENT_DATE) - DATE_PART('month', onboarding_date))
	    END) months_on_platform
FROM locations_desc		
GROUP BY work_place_id,
		 name,
		 onboarding_date;
				
	
		
CREATE TEMP TABLE bonuses AS 		
SELECT wp.id,
	   wp."name",
	   transaction_date,
	   MIN(created_when::date) first_tip_date,
	   LEAST(MIN(created_when::date), onboarding_date) bonus_start_date,
	   total_amount
FROM work_place wp LEFT JOIN
	 (SELECT workplace_payout_id,
			 created_when::date  	 transaction_date,
			 amount 				 total_amount 
	  FROM operation	
	  WHERE type = 'TIPS'
		AND status IN ('COMPLETE', 'REFUNDED')
	  GROUP BY DATE_TRUNC('day', created_when),
			   workplace_payout_id,
			   created_when,
			   amount) o ON wp.payout_id = o.workplace_payout_id 
GROUP BY wp.id,
	   	 wp."name",
	     transaction_date,
		 total_amount;		

		
SELECT id,
	   "name",
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
	     "name",
	     first_tip_date,
	     bonus_start_date
ORDER BY id;

