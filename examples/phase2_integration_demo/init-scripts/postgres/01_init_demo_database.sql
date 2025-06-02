-- SQLFlow Phase 2 Integration Demo Database Initialization
-- Creates demo database, users, and sample data for testing

-- Create demo database and user
CREATE DATABASE demo;
CREATE USER sqlflow WITH PASSWORD 'sqlflow123';
GRANT ALL PRIVILEGES ON DATABASE demo TO sqlflow;

-- Connect to demo database
\c demo;

-- Grant schema permissions
GRANT ALL ON SCHEMA public TO sqlflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO sqlflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO sqlflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO sqlflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO sqlflow;

-- Create extension for better performance monitoring
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Create sample tables for testing incremental loading
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    country VARCHAR(50) DEFAULT 'USA',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10,2) NOT NULL,
    description TEXT,
    stock_quantity INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date DATE DEFAULT CURRENT_DATE,
    total_amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    shipping_address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX idx_customers_updated_at ON customers(updated_at);
CREATE INDEX idx_products_updated_at ON products(updated_at);
CREATE INDEX idx_orders_created_at ON orders(created_at);
CREATE INDEX idx_orders_updated_at ON orders(updated_at);
CREATE INDEX idx_orders_customer_id ON orders(customer_id);

-- Insert sample customers
INSERT INTO customers (first_name, last_name, email, phone, address, city, state, zip_code, created_at, updated_at) VALUES
('John', 'Doe', 'john.doe@email.com', '555-0101', '123 Main St', 'New York', 'NY', '10001', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
('Jane', 'Smith', 'jane.smith@email.com', '555-0102', '456 Oak Ave', 'Los Angeles', 'CA', '90001', '2024-01-02 11:00:00', '2024-01-02 11:00:00'),
('Bob', 'Johnson', 'bob.johnson@email.com', '555-0103', '789 Pine St', 'Chicago', 'IL', '60601', '2024-01-03 12:00:00', '2024-01-03 12:00:00'),
('Alice', 'Williams', 'alice.williams@email.com', '555-0104', '321 Elm St', 'Houston', 'TX', '77001', '2024-01-04 13:00:00', '2024-01-04 13:00:00'),
('Charlie', 'Brown', 'charlie.brown@email.com', '555-0105', '654 Maple Ave', 'Phoenix', 'AZ', '85001', '2024-01-05 14:00:00', '2024-01-05 14:00:00');

-- Insert sample products
INSERT INTO products (product_name, category, price, description, stock_quantity, created_at, updated_at) VALUES
('Laptop Pro', 'Electronics', 1299.99, 'High-performance laptop for professionals', 50, '2024-01-01 09:00:00', '2024-01-01 09:00:00'),
('Wireless Mouse', 'Electronics', 29.99, 'Ergonomic wireless mouse', 200, '2024-01-01 09:30:00', '2024-01-01 09:30:00'),
('Office Chair', 'Furniture', 199.99, 'Comfortable ergonomic office chair', 25, '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
('Coffee Mug', 'Kitchen', 12.99, 'Ceramic coffee mug with company logo', 100, '2024-01-01 10:30:00', '2024-01-01 10:30:00'),
('Notebook Set', 'Office Supplies', 24.99, 'Set of 3 premium notebooks', 75, '2024-01-01 11:00:00', '2024-01-01 11:00:00');

-- Insert sample orders (initial batch)
INSERT INTO orders (customer_id, order_date, total_amount, status, shipping_address, created_at, updated_at) VALUES
(1, '2024-01-10', 1329.98, 'completed', '123 Main St, New York, NY 10001', '2024-01-10 14:00:00', '2024-01-10 16:00:00'),
(2, '2024-01-11', 42.98, 'shipped', '456 Oak Ave, Los Angeles, CA 90001', '2024-01-11 15:00:00', '2024-01-11 15:00:00'),
(3, '2024-01-12', 199.99, 'pending', '789 Pine St, Chicago, IL 60601', '2024-01-12 16:00:00', '2024-01-12 16:00:00');

-- Insert order items
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
(1, 1, 1, 1299.99),  -- Laptop Pro
(1, 2, 1, 29.99),    -- Wireless Mouse
(2, 2, 1, 29.99),    -- Wireless Mouse
(2, 4, 1, 12.99),    -- Coffee Mug
(3, 3, 1, 199.99);   -- Office Chair

-- Create a view for testing
CREATE VIEW customer_order_summary AS
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    COUNT(o.order_id) as total_orders,
    COALESCE(SUM(o.total_amount), 0) as total_spent,
    MAX(o.order_date) as last_order_date
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email;

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for automatic updated_at updates
CREATE TRIGGER update_customers_updated_at BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_products_updated_at BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_orders_updated_at BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Grant permissions to sqlflow user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO sqlflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO sqlflow;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO sqlflow;

-- Create additional data for incremental loading tests
-- This will be used to test incremental loading by adding new records later
CREATE TABLE incremental_test_data (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    value INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert initial batch for incremental testing
INSERT INTO incremental_test_data (name, value, created_at, updated_at) VALUES
('Initial Record 1', 100, '2024-01-15 10:00:00', '2024-01-15 10:00:00'),
('Initial Record 2', 200, '2024-01-15 11:00:00', '2024-01-15 11:00:00'),
('Initial Record 3', 300, '2024-01-15 12:00:00', '2024-01-15 12:00:00');

-- Create index for incremental loading
CREATE INDEX idx_incremental_test_data_updated_at ON incremental_test_data(updated_at);

-- Grant permissions
GRANT ALL PRIVILEGES ON incremental_test_data TO sqlflow;
GRANT ALL PRIVILEGES ON SEQUENCE incremental_test_data_id_seq TO sqlflow;

-- Display setup completion message
DO $$
BEGIN
    RAISE NOTICE '✅ SQLFlow Phase 2 Demo Database Setup Complete!';
    RAISE NOTICE '📊 Created tables: customers, products, orders, order_items, incremental_test_data';
    RAISE NOTICE '👤 Created user: sqlflow (password: sqlflow123)';
    RAISE NOTICE '🔍 Created view: customer_order_summary';
    RAISE NOTICE '⚡ Created indexes for optimal incremental loading performance';
    RAISE NOTICE '🚀 Ready for Phase 2 connector testing!';
END $$; 