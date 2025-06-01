CREATE TABLE IF NOT EXISTS sales (
  id SERIAL PRIMARY KEY,
  region TEXT,
  product TEXT,
  amount NUMERIC
);

INSERT INTO sales (region, product, amount)
VALUES
  ('NA', 'Widget', 100.5),
  ('EMEA', 'Gadget', 230.0),
  ('APAC', 'Thingamajig', 150.75);
