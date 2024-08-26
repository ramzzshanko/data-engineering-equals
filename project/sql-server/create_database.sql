-- Create the data warehouse database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create dimension tables
CREATE TABLE Date (
    date_id INT PRIMARY KEY,
    date DATE,
    day INT,
    month INT,
    year INT,
    quarter INT
);

CREATE TABLE Customer (
    customer_id INT PRIMARY KEY,
    customer_name NVARCHAR(100),
    customer_email NVARCHAR(100),
    customer_type NVARCHAR(50)
);

CREATE TABLE Product (
    product_id INT PRIMARY KEY,
    product_name NVARCHAR(100),
    product_category NVARCHAR(50),
    product_price DECIMAL(10, 2)
);

CREATE TABLE Location (
    location_id INT PRIMARY KEY,
    city NVARCHAR(50),
    state NVARCHAR(50),
    country NVARCHAR(50)
);

-- Create fact table
CREATE TABLE FinancialTransactions (
    transaction_id INT PRIMARY KEY,
    date_id INT FOREIGN KEY REFERENCES Date(date_id),
    customer_id INT FOREIGN KEY REFERENCES Customer(customer_id),
    product_id INT FOREIGN KEY REFERENCES Product(product_id),
    location_id INT FOREIGN KEY REFERENCES Location(location_id),
    transaction_amount DECIMAL(10, 2),
    transaction_type NVARCHAR(50)
);
GO