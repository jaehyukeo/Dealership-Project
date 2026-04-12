CREATE KEYSPACE IF NOT EXISTS dealership_system 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE dealership_system;

CREATE TABLE IF NOT EXISTS car (
    serial_number text PRIMARY KEY,
    model text,
    year int,
    status text
);

INSERT INTO car (serial_number, model, year, status) 
VALUES ('VIN123', 'Toyota Vios', 2024, 'Available');