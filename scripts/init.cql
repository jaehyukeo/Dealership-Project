CREATE KEYSPACE IF NOT EXISTS dealership_system 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE dealership_system;

-- 1. Car Table (Handles Rule: New/Used/Service history reference)
CREATE TABLE IF NOT EXISTS car (
    vin varchar PRIMARY KEY, -- 'Serial Number' in your problem
    brand varchar,
    model varchar,
    year int,
    type varchar,
    price decimal, 
    status varchar -- 'New', 'Used', 'Service Only'
);

-- 2. Customer Table (Handles Rule: Can exist without buying a car)
CREATE TABLE IF NOT EXISTS customer (
    customer_id int PRIMARY KEY,
    customer_name varchar,
    address varchar,
    phone varchar
);

-- 3. Salesperson Table
CREATE TABLE IF NOT EXISTS salesperson (
    salesperson_id int PRIMARY KEY,
    sp_name varchar,
    position varchar,
    branch varchar
);

-- 4. Invoice Table (Handles Rule: 1 invoice per car/customer/salesperson)
CREATE TABLE IF NOT EXISTS invoice (
    invoice_number int PRIMARY KEY,
    invoice_date timestamp,
    total_amount decimal,
    salesperson_id int,
    sp_name varchar,
    customer_id int,
    customer_name varchar,
    vin varchar,
    payment_method varchar
);

-- 5. Service Ticket (OPTIMIZED for Service History)
-- We use (vin, ticket_id) as a Composite Key so history is grouped by Car!
CREATE TABLE IF NOT EXISTS service_ticket (
    vin varchar,
    ticket_id int,
    customer_id int,
    customer_name varchar,
    mechanic_id int, -- Primary mechanic
    mechanic_name varchar,
    service_date timestamp,
    labor_cost decimal,
    parts_list list<varchar>, -- Handles Rule: May or may not need parts
    parts_cost decimal,
    total_cost decimal,
    PRIMARY KEY (vin, ticket_id) 
) WITH CLUSTERING ORDER BY (ticket_id DESC);

-- 6. Mechanic Table
CREATE TABLE IF NOT EXISTS mechanic (
    mechanic_id int PRIMARY KEY,
    mechanic_name varchar,
    specialization varchar
);