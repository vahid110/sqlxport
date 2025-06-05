CREATE TABLE logs (
  id SERIAL PRIMARY KEY,
  service TEXT NOT NULL,
  message TEXT NOT NULL,
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO logs (service, message)
SELECT
  CASE WHEN i % 2 = 0 THEN 'auth' ELSE 'billing' END,
  'Test message ' || i
FROM generate_series(1, 1000) AS i;
