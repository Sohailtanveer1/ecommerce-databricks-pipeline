-- Generate live CDC events on Postgres so you can watch Debezium capture
-- inserts (c), updates (u), and deletes (d).
--   docker exec -i v2-postgres psql -U postgres -d ecommerce < docker/generate_changes.sql

-- insert (op=c) — re-runnable
INSERT INTO orders (order_id, customer_id, amount, currency, order_status, order_date)
VALUES ('O2001','C002', 99.00,'USD','paid','2026-08-15')
ON CONFLICT (order_id) DO NOTHING;

-- update (op=u) — status change, with full before-image (REPLICA IDENTITY FULL)
UPDATE orders SET order_status='shipped', updated_at=now() WHERE order_id='O1003';

-- delete (op=d) — a cancelled order removed from source
DELETE FROM orders WHERE order_id='O1005';

-- a customer email change (SCD-relevant downstream)
UPDATE customers SET email='aisha.k@example.com', updated_at=now() WHERE customer_id='C001';
