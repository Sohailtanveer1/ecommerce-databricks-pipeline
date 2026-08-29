-- Initial snapshot rows. Debezium emits these as op='r' (read/snapshot); later
-- INSERT/UPDATE/DELETE become c/u/d events. Generate live changes with
-- docker/generate_changes.sql to see CDC in action.

INSERT INTO customers (customer_id, first_name, last_name, email, loyalty_tier, region) VALUES
 ('C001','Aisha','Khan','aisha@example.com','gold','APAC'),
 ('C002','Bruno','Silva','bruno@example.com','silver','LATAM'),
 ('C003','Chen','Wei','chen@example.com','platinum','APAC'),
 ('C004','Diana','Owusu','diana@example.com','bronze','EMEA'),
 ('C005','Ethan','Brown','ethan@example.com','gold','AMER');

INSERT INTO addresses (address_id, customer_id, line1, city, country) VALUES
 ('A001','C001','1 MG Road','Mumbai','India'),
 ('A002','C002','9 Paulista','Sao Paulo','Brazil'),
 ('A003','C003','5 Nanjing Rd','Shanghai','China'),
 ('A004','C004','22 Baker St','London','United Kingdom'),
 ('A005','C005','88 5th Ave','New York','United States');

INSERT INTO orders (order_id, customer_id, amount, currency, order_status, order_date) VALUES
 ('O1001','C001', 50.00,'USD','delivered','2026-08-01'),
 ('O1002','C004',200.50,'EUR','shipped',  '2026-08-02'),
 ('O1003','C003',269.97,'USD','paid',     '2026-08-02'),
 ('O1004','C002', 40.00,'GBP','delivered','2026-08-03'),
 ('O1005','C005', 30.00,'USD','cancelled','2026-08-04');

INSERT INTO payments (payment_id, order_id, method, amount, status) VALUES
 ('P001','O1001','card',        50.00,'captured'),
 ('P002','O1002','paypal',     200.50,'captured'),
 ('P003','O1003','bank_transfer',269.97,'captured'),
 ('P004','O1004','card',        40.00,'captured');
