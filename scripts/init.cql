CREATE KEYSPACE IF NOT EXISTS dealership_system 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE dealership_system;

-- Inventory Table
CREATE TABLE IF NOT EXISTS car (
    vin text PRIMARY KEY,
    brand text,
    model text,
    year int,
    price decimal,
    status text
);

-- Customer Table
CREATE TABLE IF NOT EXISTS customer (
    customer_id uuid PRIMARY KEY,
    first_name text,
    last_name text,
    phone text,
    email text
);

-- Salesperson Table
CREATE TABLE IF NOT EXISTS salesperson (
    emp_id text PRIMARY KEY,
    name text,
    position text,
    branch text
);

-- Sales Transaction Table
CREATE TABLE IF NOT EXISTS sales (
    sale_id uuid PRIMARY KEY,
    vin text,
    customer_name text,
    emp_name text,
    sale_date timestamp,
    final_price decimal
);

INSERT INTO car (vin, brand, model, year, price, status) 
VALUES ('VIN12345', 'Toyota', 'Vios', 2024, 850000, 'Available');

INSERT INTO salesperson (emp_id, name, position, branch) 
VALUES ('EMP001', 'Marian Obejas', 'Senior Dealer', 'Taguig');