----------------------------------------------------------------------------------------
--									logging_table											
----------------------------------------------------------------------------------------
CREATE TYPE log_type AS (
	user_name			VARCHAR(100),
	log_time			TIMESTAMP, 
	table_name			VARCHAR(200),
	operation			VARCHAR(200),	
	log_message			TEXT 	
);
--DROP TYPE logs;
--DROP TABLE bl_cl.logging;

CREATE TABLE IF NOT EXISTS bl_cl.logging(
	log_id 				SERIAL			PRIMARY KEY,
	logs				log_type
);

--DROP TABLE bl_cl.logging;
CREATE OR REPLACE PROCEDURE bl_cl.insert_logs(	table_name 	VARCHAR(100),
												operation 	VARCHAR(200),
												log_message TEXT)
LANGUAGE plpgsql
AS $$

BEGIN

INSERT INTO bl_cl.logging
            (logs)

VALUES (ROW (CURRENT_USER,
        	 CURRENT_TIMESTAMP,
        	 table_name,
        	 operation,
        	 log_message)); 

END;
$$;