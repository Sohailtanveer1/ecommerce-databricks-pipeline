-- SQL Server "ERP" — the watermark source (ADF Copy over SHIR pulls WHERE
-- modified_at > last_watermark). Every table carries modified_at for the pull.
-- Run:  sqlcmd -S localhost -U sa -P 'Str0ng!Passw0rd' -C -i /seed/01_schema.sql
IF DB_ID('erp') IS NULL CREATE DATABASE erp;
GO
USE erp;
GO

CREATE TABLE dbo.customers (
    customer_id  VARCHAR(20) PRIMARY KEY, name VARCHAR(100), email VARCHAR(120),
    segment VARCHAR(20), region VARCHAR(20),
    modified_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME());
CREATE TABLE dbo.products (
    product_id VARCHAR(20) PRIMARY KEY, product_name VARCHAR(120), category VARCHAR(60),
    unit_cost DECIMAL(12,2), modified_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME());
CREATE TABLE dbo.orders (
    order_id VARCHAR(20) PRIMARY KEY, customer_id VARCHAR(20), order_date DATE,
    amount DECIMAL(12,2), currency VARCHAR(3), order_status VARCHAR(20),
    modified_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME());
CREATE TABLE dbo.order_items (
    order_item_id VARCHAR(20) PRIMARY KEY, order_id VARCHAR(20), product_id VARCHAR(20),
    quantity INT, unit_price DECIMAL(12,2),
    modified_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME());
CREATE TABLE dbo.inventory (
    product_id VARCHAR(20), warehouse_id VARCHAR(20), on_hand INT,
    modified_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    PRIMARY KEY (product_id, warehouse_id));
CREATE TABLE dbo.returns (
    return_id VARCHAR(20) PRIMARY KEY, order_id VARCHAR(20), reason VARCHAR(120),
    refund_amount DECIMAL(12,2), modified_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME());
CREATE TABLE dbo.fx_reference (
    currency VARCHAR(3), as_of_date DATE, usd_rate DECIMAL(18,8),
    modified_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    PRIMARY KEY (currency, as_of_date));
GO

INSERT INTO dbo.customers (customer_id,name,email,segment,region) VALUES
 ('C001','Aisha Khan','aisha@example.com','consumer','APAC'),
 ('C002','Bruno Silva','bruno@example.com','consumer','LATAM'),
 ('C003','Chen Wei','chen@example.com','business','APAC'),
 ('C004','Diana Owusu','diana@example.com','consumer','EMEA'),
 ('C005','Ethan Brown','ethan@example.com','business','AMER');
INSERT INTO dbo.products (product_id,product_name,category,unit_cost) VALUES
 ('P001','Wireless Mouse','Accessories',8.00),('P002','Mechanical Keyboard','Accessories',45.00),
 ('P003','USB-C Hub','Accessories',18.00),('P004','27in Monitor','Displays',140.00),
 ('P005','Laptop Stand','Office',12.00);
INSERT INTO dbo.orders (order_id,customer_id,order_date,amount,currency,order_status) VALUES
 ('O1001','C001','2026-08-01',50.00,'USD','delivered'),('O1002','C004','2026-08-02',200.50,'EUR','shipped'),
 ('O1003','C003','2026-08-02',269.97,'USD','paid'),('O1004','C002','2026-08-03',40.00,'GBP','delivered'),
 ('O1005','C005','2026-08-04',30.00,'USD','cancelled');
INSERT INTO dbo.order_items (order_item_id,order_id,product_id,quantity,unit_price) VALUES
 ('OI1','O1001','P001',2,25.00),('OI2','O1002','P004',1,200.50),('OI3','O1003','P002',3,89.99),
 ('OI4','O1004','P003',1,40.00),('OI5','O1005','P005',1,30.00);
INSERT INTO dbo.inventory (product_id,warehouse_id,on_hand) VALUES
 ('P001','WH1',120),('P002','WH1',60),('P003','WH2',80),('P004','WH1',25),('P005','WH2',200);
INSERT INTO dbo.returns (return_id,order_id,reason,refund_amount) VALUES
 ('R1','O1004','changed mind',40.00);
INSERT INTO dbo.fx_reference (currency,as_of_date,usd_rate) VALUES
 ('USD','2026-08-01',1.0),('EUR','2026-08-01',1.09),('GBP','2026-08-01',1.27),('INR','2026-08-01',0.012);
GO

-- Read-only user for the ADF pull.
IF NOT EXISTS (SELECT 1 FROM sys.sql_logins WHERE name='erp_reader')
  CREATE LOGIN erp_reader WITH PASSWORD='Reader!Passw0rd', CHECK_POLICY=OFF;
GO
USE erp;
CREATE USER erp_reader FOR LOGIN erp_reader;
ALTER ROLE db_datareader ADD MEMBER erp_reader;
GO
