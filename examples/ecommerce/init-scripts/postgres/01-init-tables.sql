-- Initialize tables for ecommerce demo
CREATE TABLE customers (
  customer_id VARCHAR(20) PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  email VARCHAR(100) NOT NULL,
  region VARCHAR(50) NOT NULL,
  signup_date DATE NOT NULL
);

CREATE TABLE products (
  product_id VARCHAR(20) PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  category VARCHAR(50) NOT NULL,
  price DECIMAL(10,2) NOT NULL
);

CREATE TABLE sales (
  order_id VARCHAR(20) PRIMARY KEY,
  customer_id VARCHAR(20) NOT NULL REFERENCES customers(customer_id),
  product_id VARCHAR(20) NOT NULL REFERENCES products(product_id),
  quantity INTEGER NOT NULL,
  price DECIMAL(10,2) NOT NULL,
  order_date DATE NOT NULL,
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
  FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Insert sample customer data
INSERT INTO customers (customer_id, name, email, region, signup_date) VALUES
('1234', 'John Smith', 'john.smith@example.com', 'North America', '2022-01-15'),
('3456', 'Maria Garcia', 'maria.garcia@example.com', 'Europe', '2022-03-22'),
('5678', 'Wei Zhang', 'wei.zhang@example.com', 'Asia Pacific', '2021-11-05'),
('7890', 'Ahmed Hassan', 'ahmed.hassan@example.com', 'Middle East', '2022-05-10'),
('8765', 'Emma Wilson', 'emma.wilson@example.com', 'Europe', '2021-09-18'),
('9012', 'Carlos Rodriguez', 'carlos.rodriguez@example.com', 'South America', '2022-02-28');

-- Insert sample product data
INSERT INTO products (product_id, name, category, price) VALUES
('PRD-123', 'Premium Headphones', 'Electronics', 49.99),
('PRD-456', 'Ergonomic Office Chair', 'Furniture', 129.99),
('PRD-789', 'Organic Cotton T-Shirt', 'Apparel', 19.99);

-- Insert sample sales data
INSERT INTO sales (order_id, customer_id, product_id, quantity, price, order_date) VALUES
('1001', '5678', 'PRD-123', 2, 49.99, '2023-10-25'),
('1002', '8765', 'PRD-456', 1, 129.99, '2023-10-25'),
('1003', '5678', 'PRD-789', 3, 19.99, '2023-10-25'),
('1004', '9012', 'PRD-123', 1, 49.99, '2023-10-25'),
('1005', '3456', 'PRD-456', 2, 129.99, '2023-10-25'),
('1006', '7890', 'PRD-789', 5, 19.99, '2023-10-25'),
('1007', '1234', 'PRD-123', 3, 49.99, '2023-10-25'),
('1008', '5678', 'PRD-456', 1, 129.99, '2023-10-25');
