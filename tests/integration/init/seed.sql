CREATE TABLE logs (
    id SERIAL PRIMARY KEY,
    message TEXT,
    service TEXT
);

INSERT INTO logs (message, service) VALUES
('started service A', 'service_a'),
('started service B', 'service_b'),
('error in service A', 'service_a');

GRANT ALL PRIVILEGES ON TABLE logs TO testuser;

\z logs