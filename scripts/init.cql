CREATE KEYSPACE IF NOT EXISTS dealership_system 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE dealership_system;

-- Inventory Table
CREATE TABLE IF NOT EXISTS car (
    vin varchar PRIMARY KEY,
    brand varchar,
    model varchar,
    year int,
    type varchar,
    price decimal, 
    status varchar
);

-- Customer Table
CREATE TABLE IF NOT EXISTS customer (
    customer_id int PRIMARY KEY,
    customer_name varchar,
    address varchar,
    phone varchar,
    email varchar
);

-- Salesperson Table
CREATE TABLE IF NOT EXISTS salesperson (
    salesperson_id int PRIMARY KEY,
    sp_name varchar,
    position varchar,
    branch varchar
);

-- Invoice Table
CREATE TABLE IF NOT EXISTS invoice (
    invoice_number int PRIMARY KEY,
    invoice_date timestamp,
    total_amount decimal,
    sp_name varchar,
    customer_name varchar,
    car_model varchar,
    payment_method varchar
);

-- Service Ticket
CREATE TABLE IF NOT EXISTS service_ticket (
    ticket_id int PRIMARY KEY,
    vin varchar,
    customer_name varchar,
    mechanic_name varchar,
    service_date timestamp,
    labor_cost decimal,
    parts_list list<varchar>,
    parts_cost decimal,
    total_cost decimal
);

-- Mechanic Table 
CREATE TABLE IF NOT EXISTS mechanic (
    mechanic_id int PRIMARY KEY,
    mechanic_name varchar,
    specialization varchar
);

INSERT INTO car (vin, brand, model, year, type, price, status) 
VALUES ('VIN12345', 'Toyota', 'Vios', 2024, 'Sedan', 850000, 'Available');

INSERT INTO salesperson (salesperson_id, sp_name, position, branch) 
VALUES (1, 'Marian Obejas', 'Senior Dealer', 'Taguig');

INSERT INTO customer (customer_id, customer_name, address, phone, email)
VALUES (10001, 'Angelica Ballesteros', 'Upper Bicutan, Taguig', '0992724369', 'angelballesteros1228@gmail.com');

INSERT INTO mechanic (mechanic_id, mechanic_name, specialization)
VALUES (83212, 'Mark Rosario', 'Auto Diagnostic Technician');