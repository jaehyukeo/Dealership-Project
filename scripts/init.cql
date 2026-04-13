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
    model varchar, -- Car Description
    year int, -- for history
    type varchar, -- If Available or Sold
    status varchar -- New or Used
);

-- 4. Invoice Table
CREATE TABLE IF NOT EXISTS invoice (
    invoice_id int PRIMARY KEY,
    salesperson_id int,
    customer_id int,
    vin varchar,
    invoice_date timestamp,
    total_amount decimal
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

-- 6. Mechanic Table
CREATE TABLE IF NOT EXISTS mechanic (
    mechanic_id int PRIMARY KEY,
    mechanic_name varchar,
    specialization varchar
);

-- 7. ServiceTicket for Labor
CREATE TABLE IF NOT EXISTS service_ticket_labor (
    ticket_id varchar,
    service_id int,
    service_description varchar,
    mechanic_id int,
    labor_cost decimal,
    PRIMARY KEY (ticket_id, service_id)
);

-- 8. Part Master Table (Source for Inventory/Dropdown)
CREATE TABLE IF NOT EXISTS part (
    part_id int PRIMARY KEY,
    description varchar,
    unit_price decimal
);

-- 9. ServiceTicket for Parts
CREATE TABLE IF NOT EXISTS service_ticket_parts (
   ticket_id varchar,
    part_id int,
    part_name varchar,
    quantity int,
    unit_price decimal,
    PRIMARY KEY (ticket_id, part_id)
);