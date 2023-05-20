DROP TABLE IF EXISTS cdm.dm_courier_ledger;
DROP TABLE IF EXISTS cdm.dm_settlement_report;
DROP TABLE IF EXISTS cdm.srv_wf_settings;
DROP TABLE IF EXISTS dds.fct_product_sales;
DROP TABLE IF EXISTS dds.dm_orders;
DROP TABLE IF EXISTS dds.dm_couriers;
DROP TABLE IF EXISTS dds.dm_deliveries;
DROP TABLE IF EXISTS dds.dm_products;
DROP TABLE IF EXISTS dds.dm_restaurants;
DROP TABLE IF EXISTS dds.dm_users;
DROP TABLE IF EXISTS dds.dm_timestamps;
DROP TABLE IF EXISTS dds.srv_wf_settings;
DROP TABLE IF EXISTS stg.bonussystem_events;
DROP TABLE IF EXISTS stg.bonussystem_ranks;
DROP TABLE IF EXISTS stg.bonussystem_users;
DROP TABLE IF EXISTS stg.delivery_api_couriers;
DROP TABLE IF EXISTS stg.delivery_api_deliveries;
DROP TABLE IF EXISTS stg.ordersystem_orders;
DROP TABLE IF EXISTS stg.ordersystem_restaurants;
DROP TABLE IF EXISTS stg.ordersystem_users;
DROP TABLE IF EXISTS stg.srv_wf_settings;


CREATE TABLE stg.srv_wf_settings (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id),
	CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key)
);


CREATE TABLE IF NOT EXISTS stg.ordersystem_users (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_users_object_id_uindex UNIQUE (object_id),
	CONSTRAINT ordersystem_users_pkey PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS stg.ordersystem_restaurants (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_restaurants_object_id_uindex UNIQUE (object_id),
	CONSTRAINT ordersystem_restaurants_pkey PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS stg.ordersystem_orders (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_orders_object_id_uindex UNIQUE (object_id),
	CONSTRAINT ordersystem_orders_pkey PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS stg.delivery_api_deliveries (
	id serial4 NOT NULL,
	order_id varchar NOT NULL,
	object_value text NOT NULL,
	CONSTRAINT delivery_api_deliveries_order_id_key UNIQUE (order_id),
	CONSTRAINT delivery_api_deliveries_pkey PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS stg.delivery_api_couriers (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	object_value text NOT NULL,
	CONSTRAINT delivery_api_couriers_courier_id_key UNIQUE (courier_id),
	CONSTRAINT delivery_api_couriers_pkey PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS stg.bonussystem_users (
	id int4 NOT NULL,
	order_user_id text NOT NULL,
	CONSTRAINT users_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS stg.bonussystem_ranks (
	id int4 NOT NULL,
	"name" varchar(2048) NOT NULL,
	bonus_percent numeric(19, 5) NOT NULL DEFAULT 0,
	min_payment_threshold numeric(19, 5) NOT NULL DEFAULT 0,
	CONSTRAINT ranks_bonus_percent_check CHECK ((bonus_percent >= (0)::numeric)),
	CONSTRAINT ranks_bonus_percent_check1 CHECK ((bonus_percent >= (0)::numeric)),
	CONSTRAINT ranks_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS stg.bonussystem_events (
	id int4 NOT NULL,
	event_ts timestamp NOT NULL,
	event_type varchar NOT NULL,
	event_value text NOT NULL,
	CONSTRAINT id_unique UNIQUE (id)
);
CREATE INDEX idx_bonussystem_events__event_ts ON stg.bonussystem_events USING btree (event_ts);


CREATE TABLE IF NOT EXISTS dds.srv_wf_settings (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id),
	CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key)
);

CREATE TABLE dds.dm_users (
	id serial4 NOT NULL,
	user_id varchar NOT NULL,
	user_name varchar NOT NULL,
	user_login varchar NOT NULL,
	CONSTRAINT dm_users_id_un UNIQUE (id),
	CONSTRAINT dm_users_user_id_un UNIQUE (user_id)
);

CREATE TABLE IF NOT EXISTS dds.dm_timestamps (
	id serial4 NOT NULL,
	ts timestamp(0) NOT NULL,
	"year" int2 NOT NULL,
	"month" int2 NOT NULL,
	"day" int2 NOT NULL,
	"time" time NOT NULL,
	"date" date NOT NULL,
	CONSTRAINT dm_timestamps_day_check CHECK (((day >= 1) AND (day <= 31))),
	CONSTRAINT dm_timestamps_month_check CHECK (((month >= 1) AND (month <= 12))),
	CONSTRAINT dm_timestamps_pkey PRIMARY KEY (id),
	CONSTRAINT dm_timestamps_year_check CHECK (((year >= 2022) AND (year < 2500))),
	CONSTRAINT unique_ts UNIQUE (ts)
);

CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
	id serial4 NOT NULL,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT dm_restaurants_id_key UNIQUE (id),
	CONSTRAINT dm_restaurants_restaurant_id_key UNIQUE (restaurant_id)
);

CREATE TABLE IF NOT EXISTS dds.dm_products (
	id serial4 NOT NULL,
	restaurant_id int4 NOT NULL,
	product_id varchar NOT NULL,
	product_name varchar NOT NULL,
	product_price numeric(14, 2) NOT NULL DEFAULT 0,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT dm_products_id_unique UNIQUE (id),
	CONSTRAINT dm_products_product_price_check CHECK (((product_price > (0)::numeric) AND (product_price < 999000000000.99))),
	CONSTRAINT product_id_unique UNIQUE (product_id)
);


CREATE TABLE dds.dm_deliveries (
	id serial4 NOT NULL,
	delivery_id varchar NOT NULL,
	address varchar NOT NULL,
	courier_id varchar NOT NULL,
	delivery_ts timestamp NOT NULL,
	order_id varchar NOT NULL,
	order_ts timestamp NOT NULL,
	rate int4 NOT NULL,
	sum numeric(14, 2) NOT NULL,
	tip_sum numeric(14, 2) NOT NULL,
	CONSTRAINT dm_deliveries_delivery_id_key UNIQUE (delivery_id),
	CONSTRAINT dm_deliveries_pkey PRIMARY KEY (id),
	CONSTRAINT dm_deliveries_rate_check CHECK (((rate > 0) AND (rate <= 5))),
	CONSTRAINT dm_deliveries_sum_check CHECK ((sum > (0)::numeric)),
	CONSTRAINT dm_deliveries_sum_check1 CHECK ((sum > (0)::numeric))
);


CREATE TABLE dds.dm_couriers (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	CONSTRAINT dm_couriers_courier_id_key UNIQUE (courier_id)
);

CREATE TABLE dds.dm_orders (
	id serial4 NOT NULL,
	order_key varchar NOT NULL,
	order_status varchar NOT NULL,
	user_id int4 NOT NULL,
	restaurant_id int4 NOT NULL,
	timestamp_id int4 NOT NULL,
	courier_id int4 NULL,
	rate numeric NULL,
	tip_sum numeric NULL,
	CONSTRAINT dm_orders_pkey PRIMARY KEY (id),
	CONSTRAINT order_key_unique UNIQUE (order_key)
);

CREATE TABLE dds.fct_product_sales (
	id serial4 NOT NULL,
	product_id int4 NOT NULL,
	order_id int4 NOT NULL,
	count int4 NOT NULL DEFAULT 0,
	price numeric(14, 2) NOT NULL DEFAULT 0,
	total_sum numeric(14, 2) NOT NULL DEFAULT 0,
	bonus_payment numeric(14, 2) NOT NULL DEFAULT 0,
	bonus_grant numeric(14, 2) NOT NULL DEFAULT 0,
	CONSTRAINT fct_product_sales_bonus_grant_check CHECK ((bonus_grant >= (0)::numeric)),
	CONSTRAINT fct_product_sales_bonus_payment_check CHECK ((bonus_payment >= (0)::numeric)),
	CONSTRAINT fct_product_sales_count_check CHECK ((count >= 0)),
	CONSTRAINT fct_product_sales_price_check CHECK ((price >= (0)::numeric)),
	CONSTRAINT fct_product_sales_total_sum_check CHECK ((total_sum >= (0)::numeric))
);


CREATE TABLE cdm.srv_wf_settings (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id),
	CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key)
);

CREATE TABLE cdm.dm_settlement_report (
	id serial4 NOT NULL,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	settlement_date date NOT NULL,
	orders_count int4 NOT NULL DEFAULT 0,
	orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0,
	orders_bonus_payment_sum numeric(14, 2) NOT NULL DEFAULT 0,
	orders_bonus_granted_sum numeric(14, 2) NOT NULL DEFAULT 0,
	order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0,
	restaurant_reward_sum numeric(14, 2) NOT NULL DEFAULT 0,
	CONSTRAINT dm_settlement_report_order_processing_fee_check CHECK ((order_processing_fee >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_orders_bonus_granted_sum_check CHECK ((orders_bonus_granted_sum >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_orders_bonus_payment_sum_check CHECK ((orders_bonus_payment_sum >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_orders_count_check CHECK ((orders_count >= 0)),
	CONSTRAINT dm_settlement_report_orders_total_sum_check CHECK ((orders_total_sum >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_pkey PRIMARY KEY (id),
	CONSTRAINT dm_settlement_report_restaurant_reward_sum_check CHECK ((restaurant_reward_sum >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_settlement_date_check CHECK (((settlement_date >= '2022-01-01'::date) AND (settlement_date < '2500-01-01'::date))),
	CONSTRAINT dm_settlement_report_unique_restaurant_id UNIQUE (restaurant_id, settlement_date)
);

CREATE TABLE cdm.dm_courier_ledger (
	id serial4 NOT NULL,
	courier_id int4 NOT NULL,
	courier_name text NOT NULL,
	settlement_year int4 NOT NULL,
	settlement_month int4 NOT NULL,
	orders_count int4 NOT NULL,
	orders_total_sum numeric(19, 5) NOT NULL,
	rate_avg numeric(13, 3) NOT NULL,
	order_processing_fee numeric(19, 5) NOT NULL,
	courier_order_sum numeric(19, 5) NOT NULL,
	courier_tips_sum numeric(19, 5) NOT NULL,
	courier_reward_sum numeric(19, 5) NOT NULL,
	CONSTRAINT dm_courier_ledger_courier_order_sum_check CHECK ((courier_order_sum > (0)::numeric)),
	CONSTRAINT dm_courier_ledger_courier_reward_sum_check CHECK ((courier_reward_sum > (0)::numeric)),
	CONSTRAINT dm_courier_ledger_courier_tips_sum_check CHECK ((courier_tips_sum > (0)::numeric)),
	CONSTRAINT dm_courier_ledger_orders_count_check CHECK ((orders_count > 0)),
	CONSTRAINT dm_courier_ledger_orders_total_sum_check CHECK ((orders_total_sum > (0)::numeric)),
	CONSTRAINT dm_courier_ledger_orders_total_sum_check1 CHECK ((orders_total_sum > (0)::numeric)),
	CONSTRAINT dm_courier_ledger_pkey PRIMARY KEY (id),
	CONSTRAINT dm_courier_ledger_rate_avg_check CHECK (((rate_avg > (0)::numeric) AND (rate_avg <= (5)::numeric))),
	CONSTRAINT dm_courier_ledger_settlement_month_check CHECK (((settlement_month >= 1) AND (settlement_month <= 12))),
	CONSTRAINT dm_courier_ledger_settlement_year_check CHECK (((settlement_year > 2020) AND (settlement_year < 2500))),
	CONSTRAINT dm_courier_ledger_unique_cid_y_m UNIQUE (courier_id, settlement_year, settlement_month)
);

