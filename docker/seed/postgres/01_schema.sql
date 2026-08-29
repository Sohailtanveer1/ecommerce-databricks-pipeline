-- Postgres transactional source for CDC (Debezium). Logical replication is on
-- (see docker-compose command flags). REPLICA IDENTITY FULL makes UPDATE/DELETE
-- events carry the full "before" image, so downstream can apply deletes cleanly.

CREATE TABLE customers (
    customer_id  TEXT PRIMARY KEY,
    first_name   TEXT NOT NULL,
    last_name    TEXT NOT NULL,
    email        TEXT,
    loyalty_tier TEXT,
    region       TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE addresses (
    address_id   TEXT PRIMARY KEY,
    customer_id  TEXT REFERENCES customers(customer_id),
    line1        TEXT,
    city         TEXT,
    country      TEXT,
    is_primary   BOOLEAN DEFAULT true,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id     TEXT PRIMARY KEY,
    customer_id  TEXT REFERENCES customers(customer_id),
    amount       NUMERIC(12,2) NOT NULL,
    currency     TEXT NOT NULL DEFAULT 'USD',
    order_status TEXT NOT NULL,
    order_date   DATE NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE payments (
    payment_id   TEXT PRIMARY KEY,
    order_id     TEXT REFERENCES orders(order_id),
    method       TEXT,
    amount       NUMERIC(12,2),
    status       TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE addresses REPLICA IDENTITY FULL;
ALTER TABLE orders    REPLICA IDENTITY FULL;
ALTER TABLE payments  REPLICA IDENTITY FULL;

-- Publication Debezium subscribes to (pgoutput plugin).
CREATE PUBLICATION dbz_publication FOR ALL TABLES;

-- Least-privilege CDC user with replication.
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='debezium') THEN
    CREATE ROLE debezium WITH LOGIN PASSWORD 'dbz' REPLICATION;
  END IF;
END $$;
GRANT CONNECT ON DATABASE ecommerce TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;
