-- customers table
CREATE TABLE customers (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    country TEXT NOT NULL
);

-- orders table
CREATE TABLE orders (
    id INT PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(id),
    total_amount NUMERIC(10,2),
    created_at TIMESTAMP NOT NULL
);

-- payments table
CREATE TABLE payments (
    id INT PRIMARY KEY,
    order_id INT NOT NULL REFERENCES orders(id),
    amount NUMERIC(10,2),
    method TEXT NOT NULL
);

-- seed data
INSERT INTO customers VALUES
(1, 'Alice', 'Germany'),
(2, 'Bob', 'France'),
(3, 'Charlie', 'Germany');

INSERT INTO orders VALUES
(101, 1, 100.50, '2024-01-01'),
(102, 2, 200.00, '2024-02-10'),
(103, 3, 150.00, '2024-03-15'),
(104, 1, 75.00,  '2024-04-05');

INSERT INTO payments VALUES
(1001, 101, 100.50, 'card'),
(1002, 102, 100.00, 'paypal'),
(1003, 102, 100.00, 'voucher'),
(1004, 104, 75.00, 'cash');
