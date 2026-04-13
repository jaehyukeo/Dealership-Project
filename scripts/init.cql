CREATE KEYSPACE IF NOT EXISTS dealership_system 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE dealership_system;

-- 1. Salesperson Table
CREATE TABLE IF NOT EXISTS salesperson (
    salesperson_id int PRIMARY KEY,
    sp_name varchar,
    phone varchar
);

-- 2. Customer Table
CREATE TABLE IF NOT EXISTS customer (
    customer_id int PRIMARY KEY,
    customer_name varchar,
    address varchar,
    phone varchar
);

-- 3. Car Table
CREATE TABLE IF NOT EXISTS car (
    vin varchar PRIMARY KEY, 
    price decimal, 
    status varchar -- New or Used
);

-- 4. Invoice Table
CREATE TABLE IF NOT EXISTS invoice (
    invoice_id int PRIMARY KEY,
    invoice_date timestamp,
    total_amount decimal,
    salesperson_id int,
    customer_id int,
    vin varchar
);

-- 5. Service Ticket 
CREATE TABLE IF NOT EXISTS service_ticket (
    vin varchar,
    ticket_id varchar, 
    customer_id int,
    service_date timestamp,
    total_amount decimal,
    PRIMARY KEY (vin, ticket_id) 
) WITH CLUSTERING ORDER BY (ticket_id DESC);

-- 5.A. ServiceTicket for Labor
CREATE TABLE IF NOT EXISTS service_ticket_labor (
    ticket_id varchar,
    service_id int,
    service_description varchar,
    mechanic_id int,
    labor_cost decimal,
    PRIMARY KEY (ticket_id, service_id)
);

-- 6. Mechanic Table
CREATE TABLE IF NOT EXISTS mechanic (
    mechanic_id int PRIMARY KEY,
    mechanic_name varchar,
    specialization varchar
);

--5.B. ServiceTicket for Parts
CREATE TABLE IF NOT EXISTS service_ticket_parts (
    ticket_id varchar,
    part_id int,
    part_name varchar,
    quantity int,
    unit_price decimal,
    PRIMARY KEY (ticket_id, part_id)
);