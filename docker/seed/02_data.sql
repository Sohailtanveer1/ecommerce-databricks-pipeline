-- Sample data. Multiple currencies (USD/EUR/GBP) so the REST FX source has a
-- real job: normalizing revenue to USD in the Gold layer.

INSERT INTO customers (customer_id, name, email, phone, region, customer_segment) VALUES
 ('C001', 'Aisha Khan',      'aisha@example.com',   '+91-9000000001', 'APAC', 'consumer'),
 ('C002', 'Bruno Silva',     'bruno@example.com',   '+55-1100000002', 'LATAM','consumer'),
 ('C003', 'Chen Wei',        'chen@example.com',    '+86-1300000003', 'APAC', 'business'),
 ('C004', 'Diana Owusu',     'diana@example.com',   '+44-7000000004', 'EMEA', 'consumer'),
 ('C005', 'Ethan Brown',     'ethan@example.com',   '+1-2120000005',  'AMER', 'business')
ON CONFLICT (customer_id) DO NOTHING;

INSERT INTO products (product_id, product_name, category) VALUES
 ('P001', 'Wireless Mouse',       'Accessories'),
 ('P002', 'Mechanical Keyboard',  'Accessories'),
 ('P003', 'USB-C Hub',            'Accessories'),
 ('P004', '27in Monitor',         'Displays'),
 ('P005', 'Laptop Stand',         'Office')
ON CONFLICT (product_id) DO NOTHING;

INSERT INTO orders (order_id, customer_id, product_id, order_date, amount, currency, order_status) VALUES
 ('O1001','C001','P001','2026-08-01', 25.00,'USD','delivered'),
 ('O1002','C002','P004','2026-08-02',210.50,'EUR','shipped'),
 ('O1003','C003','P002','2026-08-02', 89.99,'USD','paid'),
 ('O1004','C004','P003','2026-08-03', 45.00,'GBP','delivered'),
 ('O1005','C005','P005','2026-08-04', 30.00,'USD','cancelled'),
 ('O1006','C001','P004','2026-08-05',199.00,'EUR','paid'),
 ('O1007','C002','P001','2026-08-05', 25.00,'USD','returned')
ON CONFLICT (order_id) DO NOTHING;

-- Read-only user for the pipeline (least privilege: SELECT only).
DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ecom_reader') THEN
      CREATE ROLE ecom_reader LOGIN PASSWORD 'change-me-strong-password';
   END IF;
END
$$;

GRANT CONNECT ON DATABASE ecommerce TO ecom_reader;
GRANT USAGE ON SCHEMA public TO ecom_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ecom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ecom_reader;
