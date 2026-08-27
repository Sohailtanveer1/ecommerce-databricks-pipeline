-- Sample data. Multiple currencies (USD/EUR/GBP/INR) so the REST FX source has a
-- real job: normalizing revenue to USD in Gold.

INSERT INTO customers (customer_id, first_name, last_name, email, phone, country, city, region, customer_segment, loyalty_tier, signup_date, marketing_opt_in) VALUES
 ('C001','Aisha','Khan',    'aisha@example.com',  '+91-9000000001','India',        'Mumbai',    'APAC', 'consumer','gold',    '2024-02-11', true),
 ('C002','Bruno','Silva',   'bruno@example.com',  '+55-1100000002','Brazil',       'Sao Paulo', 'LATAM','consumer','silver',  '2024-06-03', false),
 ('C003','Chen','Wei',      'chen@example.com',   '+86-1300000003','China',        'Shanghai',  'APAC', 'business','platinum','2023-11-20', true),
 ('C004','Diana','Owusu',   'diana@example.com',  '+44-7000000004','United Kingdom','London',   'EMEA', 'consumer','bronze',  '2025-01-15', true),
 ('C005','Ethan','Brown',   'ethan@example.com',  '+1-2120000005', 'United States','New York',  'AMER', 'business','gold',    '2023-08-09', false),
 ('C006','Farah','Haddad',  'farah@example.com',  '+971-500000006','UAE',          'Dubai',     'EMEA', 'consumer','silver',  '2024-09-27', true),
 ('C007','Giorgio','Rossi', 'giorgio@example.com','+39-3400000007','Italy',        'Milan',     'EMEA', 'consumer','bronze',  '2025-03-02', false),
 ('C008','Hana','Kim',      'hana@example.com',   '+82-1000000008','South Korea',  'Seoul',     'APAC', 'business','gold',    '2024-04-18', true),
 ('C009','Ivan','Petrov',   'ivan@example.com',   '+1-4150000009', 'United States','San Jose',  'AMER', 'consumer','silver',  '2025-05-21', true),
 ('C010','Julia','Meyer',   'julia@example.com',  '+49-1510000010','Germany',      'Berlin',    'EMEA', 'business','platinum','2023-12-30', false)
ON CONFLICT (customer_id) DO NOTHING;

INSERT INTO products (product_id, product_name, category, subcategory, brand, unit_cost) VALUES
 ('P001','Wireless Mouse',       'Accessories','Input Devices','Logi',    8.00),
 ('P002','Mechanical Keyboard',  'Accessories','Input Devices','Keychron',45.00),
 ('P003','USB-C Hub',            'Accessories','Adapters',     'Anker',   18.00),
 ('P004','27in 4K Monitor',      'Displays',   'Monitors',     'Dell',   140.00),
 ('P005','Laptop Stand',         'Office',     'Ergonomics',   'Rain',    12.00),
 ('P006','Noise-Cancel Headset', 'Audio',      'Headphones',   'Sony',    90.00),
 ('P007','Webcam 1080p',         'Accessories','Cameras',      'Logi',    22.00),
 ('P008','Standing Desk',        'Office',     'Furniture',    'Uplift', 210.00)
ON CONFLICT (product_id) DO NOTHING;

INSERT INTO orders (order_id, customer_id, product_id, quantity, unit_price, discount, amount, currency, payment_method, order_channel, order_status, order_date, shipping_country) VALUES
 ('O1001','C001','P001',2, 25.00, 0.00,  50.00,'USD','card',         'web',        'delivered','2026-08-01','India'),
 ('O1002','C004','P004',1,210.50,10.00, 200.50,'EUR','paypal',       'web',        'shipped',  '2026-08-02','United Kingdom'),
 ('O1003','C003','P002',3, 89.99, 0.00, 269.97,'USD','bank_transfer','marketplace','paid',     '2026-08-02','China'),
 ('O1004','C004','P003',1, 45.00, 5.00,  40.00,'GBP','card',         'mobile_app', 'delivered','2026-08-03','United Kingdom'),
 ('O1005','C005','P005',1, 30.00, 0.00,  30.00,'USD','card',         'web',        'cancelled','2026-08-04','United States'),
 ('O1006','C001','P004',1,199.00, 0.00, 199.00,'EUR','card',         'web',        'paid',     '2026-08-05','India'),
 ('O1007','C002','P001',1, 25.00, 0.00,  25.00,'USD','cod',          'mobile_app', 'returned', '2026-08-05','Brazil'),
 ('O1008','C006','P006',2,120.00,20.00, 220.00,'USD','card',         'web',        'delivered','2026-08-06','UAE'),
 ('O1009','C007','P002',1, 89.99, 0.00,  89.99,'EUR','paypal',       'web',        'shipped',  '2026-08-06','Italy'),
 ('O1010','C008','P008',1,349.00, 0.00, 349.00,'USD','bank_transfer','marketplace','paid',     '2026-08-07','South Korea'),
 ('O1011','C009','P007',3, 40.00, 0.00, 120.00,'USD','card',         'mobile_app', 'delivered','2026-08-07','United States'),
 ('O1012','C010','P004',2,199.00, 0.00, 398.00,'EUR','card',         'web',        'delivered','2026-08-08','Germany'),
 ('O1013','C003','P006',1,120.00, 0.00, 120.00,'USD','bank_transfer','web',        'paid',     '2026-08-08','China'),
 ('O1014','C004','P001',2, 25.00, 2.00,  48.00,'GBP','card',         'mobile_app', 'shipped',  '2026-08-09','United Kingdom'),
 ('O1015','C001','P003',1, 45.00, 0.00,  45.00,'INR','card',         'web',        'delivered','2026-08-09','India'),
 ('O1016','C005','P008',1,349.00,50.00, 299.00,'USD','card',         'web',        'paid',     '2026-08-10','United States'),
 ('O1017','C006','P005',4, 30.00, 0.00, 120.00,'USD','paypal',       'marketplace','delivered','2026-08-10','UAE'),
 ('O1018','C010','P002',1, 89.99, 0.00,  89.99,'EUR','bank_transfer','web',        'cancelled','2026-08-11','Germany'),
 ('O1019','C008','P007',2, 40.00, 0.00,  80.00,'USD','card',         'mobile_app', 'delivered','2026-08-11','South Korea'),
 ('O1020','C002','P006',1,120.00, 0.00, 120.00,'USD','cod',          'web',        'shipped',  '2026-08-12','Brazil'),
 ('O1021','C007','P004',1,199.00, 0.00, 199.00,'EUR','paypal',       'web',        'paid',     '2026-08-12','Italy'),
 ('O1022','C009','P001',1, 25.00, 0.00,  25.00,'USD','card',         'mobile_app', 'delivered','2026-08-13','United States'),
 ('O1023','C003','P008',2,349.00, 0.00, 698.00,'USD','bank_transfer','marketplace','paid',     '2026-08-13','China'),
 ('O1024','C001','P006',1,120.00,10.00, 110.00,'INR','card',         'web',        'delivered','2026-08-14','India')
ON CONFLICT (order_id) DO NOTHING;

-- Read-only user for the pipeline (least privilege: SELECT only). Password is
-- reset to the Key Vault value after container init.
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
