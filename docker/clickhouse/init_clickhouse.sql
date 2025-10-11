CREATE DATABASE IF NOT EXISTS piccha;

CREATE TABLE IF NOT EXISTS piccha.raw_stores (
    ts DateTime DEFAULT now(),
    source String,
    raw_json String
) ENGINE = MergeTree() ORDER BY ts;

CREATE TABLE IF NOT EXISTS piccha.raw_products (
    ts DateTime DEFAULT now(),
    source String,
    raw_json String
) ENGINE = MergeTree() ORDER BY ts;

CREATE TABLE IF NOT EXISTS piccha.raw_customers (
    ts DateTime DEFAULT now(),
    source String,
    raw_json String
) ENGINE = MergeTree() ORDER BY ts;

CREATE TABLE IF NOT EXISTS piccha.raw_purchases (
    ts DateTime DEFAULT now(),
    source String,
    raw_json String
) ENGINE = MergeTree() ORDER BY ts;

-- Customers
CREATE TABLE IF NOT EXISTS piccha.customers (
    customer_id String,
    first_name String,
    last_name String,
    email_masked String,
    phone_masked String,
    gender String,
    birth_date Nullable(Date),
    registration_date Nullable(DateTime),
    is_loyalty_member UInt8,
    loyalty_card_number String,
    city String,
    street String,
    house String,
    postal_code String,
    ins_ts DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY customer_id;


-- Products
CREATE TABLE IF NOT EXISTS piccha.products (
    product_id String,
    name String,
    category String,
    description String,
    calories Float32,
    protein Float32,
    fat Float32,
    carbohydrates Float32,
    price Float32,
    unit String,
    origin_country String,
    expiry_days UInt16,
    is_organic UInt8,
    barcode String,
    manufacturer_name String,
    manufacturer_country String,
    manufacturer_website String,
    manufacturer_inn String,
    ins_ts DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY product_id;


-- Stores
CREATE TABLE IF NOT EXISTS piccha.stores (
    store_id String,
    store_name String,
    store_network String,
    store_type_description String,
    type String,
    city String,
    street String,
    house String,
    postal_code String,
    manager_name String,
    manager_phone String,
    manager_email String,
    accepts_online_orders UInt8,
    delivery_available UInt8,
    warehouse_connected UInt8,
    last_inventory_date Nullable(Date),
    ins_ts DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY store_id;


-- Purchases
CREATE TABLE IF NOT EXISTS piccha.purchases (
    purchase_id String,
    customer_id String,
    store_id String,
    total_amount Float32,
    payment_method String,
    is_delivery UInt8,
    purchase_datetime Nullable(DateTime),
    delivery_country Nullable(String),
    delivery_city Nullable(String),
    delivery_street Nullable(String),
    delivery_house Nullable(String),
    delivery_apartment Nullable(String),
    delivery_postal_code Nullable(String),
    items_json String,
    ins_ts DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY purchase_id;



--mv_tables
CREATE MATERIALIZED VIEW IF NOT EXISTS piccha.mv_customers
TO piccha.customers
AS
SELECT
    JSON_VALUE(raw_json, '$.customer_id') AS customer_id,
    JSON_VALUE(raw_json, '$.first_name') AS first_name,
    JSON_VALUE(raw_json, '$.last_name') AS last_name,
    JSON_VALUE(raw_json, '$.email_masked') AS email_masked,
    JSON_VALUE(raw_json, '$.phone_masked') AS phone_masked,
    JSON_VALUE(raw_json, '$.gender') AS gender,
    toDateOrNull(JSON_VALUE(raw_json, '$.birth_date')) AS birth_date,
    parseDateTimeBestEffort(JSON_VALUE(raw_json, '$.registration_date')) AS registration_date,
    multiIf(lower(JSON_VALUE(raw_json, '$.is_loyalty_member')) = 'true', 1, 0) AS is_loyalty_member,
    JSON_VALUE(raw_json, '$.loyalty_card_number') AS loyalty_card_number,
    JSON_VALUE(raw_json, '$.purchase_location.city') AS city,
    JSON_VALUE(raw_json, '$.purchase_location.street') AS street,
    JSON_VALUE(raw_json, '$.purchase_location.house') AS house,
    JSON_VALUE(raw_json, '$.purchase_location.postal_code') AS postal_code,
    now() AS ins_ts
FROM piccha.raw_customers;


CREATE MATERIALIZED VIEW IF NOT EXISTS piccha.mv_products
TO piccha.products
AS
SELECT
    JSON_VALUE(raw_json, '$.id') AS product_id,
    JSON_VALUE(raw_json, '$.name') AS name,
    JSON_VALUE(raw_json, '$.group') AS category,
    JSON_VALUE(raw_json, '$.description') AS description,
    toFloat64OrNull(JSON_VALUE(raw_json, '$.kbju.calories')) AS calories,
    toFloat64OrNull(JSON_VALUE(raw_json, '$.kbju.protein')) AS protein,
    toFloat64OrNull(JSON_VALUE(raw_json, '$.kbju.fat')) AS fat,
    toFloat64OrNull(JSON_VALUE(raw_json, '$.kbju.carbohydrates')) AS carbohydrates,
    toFloat64OrNull(JSON_VALUE(raw_json, '$.price')) AS price,
    JSON_VALUE(raw_json, '$.unit') AS unit,
    JSON_VALUE(raw_json, '$.origin_country') AS origin_country,
    toUInt16OrNull(JSON_VALUE(raw_json, '$.expiry_days')) AS expiry_days,
    multiIf(lower(JSON_VALUE(raw_json, '$.is_organic')) = 'true', 1, 0) AS is_organic,
    JSON_VALUE(raw_json, '$.barcode') AS barcode,
    JSON_VALUE(raw_json, '$.manufacturer.name') AS manufacturer_name,
    JSON_VALUE(raw_json, '$.manufacturer.country') AS manufacturer_country,
    JSON_VALUE(raw_json, '$.manufacturer.website') AS manufacturer_website,
    JSON_VALUE(raw_json, '$.manufacturer.inn') AS manufacturer_inn,
    now() AS ins_ts
FROM piccha.raw_products;


CREATE MATERIALIZED VIEW IF NOT EXISTS piccha.mv_purchases
TO piccha.purchases
AS
SELECT
    JSON_VALUE(raw_json, '$.purchase_id') AS purchase_id,
    JSON_VALUE(raw_json, '$.customer.customer_id') AS customer_id,
    JSON_VALUE(raw_json, '$.store.store_id') AS store_id,
    toFloat64OrNull(JSON_VALUE(raw_json, '$.total_amount')) AS total_amount,
    JSON_VALUE(raw_json, '$.payment_method') AS payment_method,
    multiIf(lower(JSON_VALUE(raw_json, '$.is_delivery')) = 'true', 1, 0) AS is_delivery,
    parseDateTimeBestEffortOrNull(JSON_VALUE(raw_json, '$.purchase_datetime')) AS purchase_datetime,
    JSON_VALUE(raw_json, '$.delivery_address.country') AS delivery_country,
    JSON_VALUE(raw_json, '$.delivery_address.city') AS delivery_city,
    JSON_VALUE(raw_json, '$.delivery_address.street') AS delivery_street,
    JSON_VALUE(raw_json, '$.delivery_address.house') AS delivery_house,
    JSON_VALUE(raw_json, '$.delivery_address.apartment') AS delivery_apartment,
    JSON_VALUE(raw_json, '$.delivery_address.postal_code') AS delivery_postal_code,
    JSON_QUERY(raw_json, '$.items') AS items_json,
    now() AS ins_ts
FROM piccha.raw_purchases;


CREATE MATERIALIZED VIEW IF NOT EXISTS piccha.mv_stores
TO piccha.stores
AS
SELECT
    JSON_VALUE(raw_json, '$.store_id') AS store_id,
    JSON_VALUE(raw_json, '$.store_name') AS store_name,
    JSON_VALUE(raw_json, '$.store_network') AS store_network,
    JSON_VALUE(raw_json, '$.store_type_description') AS store_type_description,
    JSON_VALUE(raw_json, '$.type') AS type,
    JSON_VALUE(raw_json, '$.location.city') AS city,
    JSON_VALUE(raw_json, '$.location.street') AS street,
    JSON_VALUE(raw_json, '$.location.house') AS house,
    JSON_VALUE(raw_json, '$.location.postal_code') AS postal_code,
    JSON_VALUE(raw_json, '$.manager.name') AS manager_name,
    JSON_VALUE(raw_json, '$.manager.phone') AS manager_phone,
    JSON_VALUE(raw_json, '$.manager.email') AS manager_email,
    multiIf(lower(JSON_VALUE(raw_json, '$.accepts_online_orders')) = 'true', 1, 0) AS accepts_online_orders,
    multiIf(lower(JSON_VALUE(raw_json, '$.delivery_available')) = 'true', 1, 0) AS delivery_available,
    multiIf(lower(JSON_VALUE(raw_json, '$.warehouse_connected')) = 'true', 1, 0) AS warehouse_connected,
    toDateOrNull(JSON_VALUE(raw_json, '$.last_inventory_date')) AS last_inventory_date,
    now() AS ins_ts
FROM piccha.raw_stores;


--clean_tables
CREATE TABLE IF NOT EXISTS piccha.clean_customers
(
    customer_id String,
    first_name String,
    last_name String,
    email_masked String,
    phone_masked String,
    gender String,
    birth_date Nullable(Date),
    registration_date Nullable(DateTime),
    loyalty_card_number String,
    is_loyalty_member UInt8,
    city String,
    street String,
    house String,
    postal_code String,
    ins_ts DateTime
) ENGINE = MergeTree()
ORDER BY customer_id;


CREATE TABLE IF NOT EXISTS piccha.clean_products
(
    product_id String,
    name String,
    category String,
    description String,
    calories Float64,
    protein Float64,
    fat Float64,
    carbohydrates Float64,
    price Float64,
    unit String,
    origin_country String,
    expiry_days Int32,
    is_organic UInt8,
    barcode String,
    manufacturer_name String,
    manufacturer_country String,
    manufacturer_website String,
    manufacturer_inn String,
    ins_ts DateTime
) ENGINE = MergeTree()
ORDER BY product_id;


CREATE TABLE IF NOT EXISTS piccha.clean_stores
(
    store_id String,
    store_name String,
    store_network String,
    city String,
    street String,
    house String,
    postal_code String,
    manager_name String,
    manager_phone String,
    manager_email String,
    accepts_online_orders UInt8,
    delivery_available UInt8,
    warehouse_connected UInt8,
    last_inventory_date Nullable(DateTime),
    ins_ts DateTime
) ENGINE = MergeTree()
ORDER BY store_id;


CREATE TABLE IF NOT EXISTS piccha.clean_purchases
(
    purchase_id String,
    customer_id String,
    store_id String,
    total_amount Float64,
    payment_method String,
    is_delivery UInt8,
    purchase_datetime DateTime,
    delivery_country String,
    delivery_city String,
    delivery_street String,
    delivery_house String,
    delivery_apartment String,
    delivery_postal_code String,
    items_json String,
    ins_ts DateTime
) ENGINE = MergeTree()
ORDER BY purchase_id;


CREATE MATERIALIZED VIEW IF NOT EXISTS piccha.mv_clean_customers
TO piccha.clean_customers
AS
SELECT DISTINCT
    customer_id,
    lower(first_name) AS first_name,
    lower(last_name) AS last_name,
    email_masked,
    phone_masked,
    lower(gender) AS gender,
    birth_date,
    registration_date,
    is_loyalty_member,
    loyalty_card_number,
    city,
    street,
    house,
    postal_code,
    ins_ts
FROM piccha.customers
WHERE customer_id IS NOT NULL
  AND birth_date <= today()
  AND registration_date <= now();


CREATE MATERIALIZED VIEW IF NOT EXISTS piccha.mv_clean_products
TO piccha.clean_products
AS
SELECT DISTINCT
    product_id,
    lower(name) AS name,
    lower(category) AS category,
    lower(description) AS description,
    calories,
    protein,
    fat,
    carbohydrates,
    price,
    lower(unit) AS unit,
    lower(origin_country) AS origin_country,
    expiry_days,
    is_organic,
    barcode,
    lower(manufacturer_name) AS manufacturer_name,
    lower(manufacturer_country) AS manufacturer_country,
    manufacturer_website,
    manufacturer_inn,
    ins_ts
FROM piccha.products
WHERE product_id IS NOT NULL
  AND price >= 0;


CREATE MATERIALIZED VIEW IF NOT EXISTS piccha.mv_clean_stores
TO piccha.clean_stores
AS
SELECT DISTINCT
    store_id,
    lower(store_name) AS store_name,
    lower(store_network) AS store_network,
    lower(city) AS city,
    lower(street) AS street,
    house,
    postal_code,
    lower(manager_name) AS manager_name,
    manager_phone,
    manager_email,
    accepts_online_orders,
    delivery_available,
    warehouse_connected,
    last_inventory_date,
    ins_ts
FROM piccha.stores
WHERE store_id IS NOT NULL;


CREATE MATERIALIZED VIEW IF NOT EXISTS piccha.mv_clean_purchases
TO piccha.clean_purchases
AS
SELECT DISTINCT
    purchase_id,
    customer_id,
    store_id,
    total_amount,
    lower(payment_method) AS payment_method,
    is_delivery,
    purchase_datetime,
    delivery_country,
    delivery_city,
    delivery_street,
    delivery_house,
    delivery_apartment,
    delivery_postal_code,
    items_json,
    ins_ts
FROM piccha.purchases
WHERE purchase_id IS NOT NULL
  AND purchase_datetime <= now()
  AND total_amount >= 0;


--mart
CREATE DATABASE IF NOT EXISTS piccha;

CREATE TABLE IF NOT EXISTS piccha.mart_customer_agg
(
    customer_id String,
    purchase_count UInt32,
    total_spent Float64,
    avg_basket Float64,
    first_purchase Nullable(DateTime),
    last_purchase Nullable(DateTime),
    distinct_stores UInt32,
    cash_count UInt32,
    card_count UInt32,
    delivery_count UInt32,
    new_customer UInt8,
    is_loyalty_member UInt8,
    registration_date Nullable(DateTime),
    ins_ts DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY customer_id;

TRUNCATE TABLE piccha.mart_customer_agg;

INSERT INTO piccha.mart_customer_agg
SELECT
    customer_id,
    p.purchase_count,
    p.total_spent,
    p.avg_basket,
    p.first_purchase,
    p.last_purchase,
    p.distinct_stores,
    p.cash_count,
    p.card_count,
    p.delivery_count,
    multiIf(c.registration_date IS NULL, 0,
            multiIf(dateDiff('day', toDate(c.registration_date), today()) < 30, 1, 0)) AS new_customer,
    coalesce(c.is_loyalty_member, 0) AS is_loyalty_member,
    c.registration_date,
    now() AS ins_ts
FROM
(
    SELECT
        customer_id,
        count() AS purchase_count,
        sum(total_amount) AS total_spent,
        avg(total_amount) AS avg_basket,
        min(purchase_datetime) AS first_purchase,
        max(purchase_datetime) AS last_purchase,
        uniqExact(store_id) AS distinct_stores,
        sumIf(1, payment_method = 'cash') AS cash_count,
        sumIf(1, payment_method = 'card') AS card_count,
        sum(is_delivery) AS delivery_count
    FROM piccha.clean_purchases
    GROUP BY customer_id
) AS p
LEFT JOIN piccha.clean_customers AS c
    ON p.customer_id = c.customer_id;