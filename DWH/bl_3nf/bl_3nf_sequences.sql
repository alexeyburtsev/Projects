CREATE OR REPLACE PROCEDURE bl_cl.create_3nf_seq()
LANGUAGE plpgsql
AS $$
BEGIN

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_regions
START WITH 1 
INCREMENT BY 1 
MINVALUE 1 
NO MAXVALUE;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_countries
START WITH 1 
INCREMENT BY 1 
MINVALUE 1 
NO MAXVALUE;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_cities
START WITH 1 
INCREMENT BY 1 
MINVALUE 1 
NO MAXVALUE;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_addresses
START WITH 1 
INCREMENT BY 1 
MINVALUE 1 
NO MAXVALUE;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_customers
START WITH 1 
INCREMENT BY 1 
MINVALUE 1 
NO MAXVALUE;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_warehouses
START WITH 1 
INCREMENT BY 1 
MINVALUE 1 
NO MAXVALUE;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_employees
START WITH 1 
INCREMENT BY 1 
MINVALUE 1 
NO MAXVALUE;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_promotions
START WITH 1 
INCREMENT BY 1 
MINVALUE 1 
NO MAXVALUE;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_deliveries
START WITH 1 
INCREMENT BY 1 
MINVALUE 1 
NO MAXVALUE;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_categories
START WITH 1 
INCREMENT BY 1 
MINVALUE 1 
NO MAXVALUE;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_payment_types
START WITH 1 
INCREMENT BY 1 
MINVALUE 1 
NO MAXVALUE;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_products
START WITH 1 
INCREMENT BY 1 
MINVALUE 1 
NO MAXVALUE;

END; $$; 






