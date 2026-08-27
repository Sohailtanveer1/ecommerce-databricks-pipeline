-- Transactional "ERP" schema (the JDBC source). Enriched with the customer and
-- order attributes a real small e-commerce company would track.
-- `updated_at` powers ADF's watermark-based incremental Copy.

CREATE TABLE IF NOT EXISTS customers (
    customer_id      TEXT PRIMARY KEY,
    first_name       TEXT NOT NULL,
    last_name        TEXT NOT NULL,
    email            TEXT,
    phone            TEXT,
    country          TEXT,
    city             TEXT,
    region           TEXT,               -- APAC / EMEA / AMER / LATAM
    customer_segment TEXT,               -- consumer / business
    loyalty_tier     TEXT,               -- bronze / silver / gold / platinum
    signup_date      DATE,
    marketing_opt_in BOOLEAN DEFAULT false,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS products (
    product_id   TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    category     TEXT,
    subcategory  TEXT,
    brand        TEXT,
    unit_cost    NUMERIC(12,2),          -- what it costs us (for margin analysis)
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id       TEXT PRIMARY KEY,
    customer_id    TEXT REFERENCES customers(customer_id),
    product_id     TEXT REFERENCES products(product_id),
    quantity       INT NOT NULL DEFAULT 1,
    unit_price     NUMERIC(12,2) NOT NULL,
    discount       NUMERIC(12,2) NOT NULL DEFAULT 0,
    amount         NUMERIC(12,2) NOT NULL,   -- net = quantity*unit_price - discount
    currency       TEXT NOT NULL DEFAULT 'USD',
    payment_method TEXT,                     -- card / paypal / bank_transfer / cod
    order_channel  TEXT,                     -- web / mobile_app / marketplace
    order_status   TEXT NOT NULL,            -- pending/paid/shipped/delivered/cancelled/returned
    order_date     DATE NOT NULL,
    shipping_country TEXT,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orders_updated_at ON orders(updated_at);
CREATE INDEX IF NOT EXISTS idx_customers_updated_at ON customers(updated_at);
CREATE INDEX IF NOT EXISTS idx_products_updated_at ON products(updated_at);
