INSERT INTO users (name, email) VALUES ('Charlie Green', 'charlie@example.com');
INSERT INTO users (name, email) VALUES ('Diana White', 'diana@example.com');

INSERT INTO orders (user_id, product_name, quantity) VALUES (1, 'Product F', 7);
INSERT INTO orders (user_id, product_name, quantity) VALUES (5, 'Product G', 2);

UPDATE users SET email = 'john.doe@example.com' WHERE id = 1;
UPDATE orders SET quantity = 10 WHERE id = 1;

DELETE FROM orders WHERE id = 2;
