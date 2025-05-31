CREATE TABLE sales (
    id SERIAL PRIMARY KEY,
    product VARCHAR(50),
    region VARCHAR(50),
    amount INT,
    sale_date DATE
);

INSERT INTO sales (product, region, amount, sale_date) VALUES
('Widget A', 'EMEA', 1200, '2024-05-01'),
('Widget B', 'NA', 800, '2024-05-02'),
('Widget C', 'APAC', 1500, '2024-05-03');
