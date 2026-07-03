CREATE TABLE IF NOT EXISTS products (
    product_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    price_amount NUMERIC(14, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    category TEXT NOT NULL,
    brand TEXT NOT NULL,
    available INTEGER NOT NULL,
    reserved INTEGER NOT NULL,
    sku TEXT NOT NULL,
    store_id TEXT NOT NULL,
    data JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS products_name_search_idx
    ON products USING gin (to_tsvector('simple', name));

CREATE TABLE IF NOT EXISTS recommendations (
    user_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    product_name TEXT NOT NULL,
    reason TEXT NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (user_id, product_id)
);

CREATE TABLE IF NOT EXISTS client_requests (
    request_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    request_type TEXT NOT NULL,
    query TEXT,
    requested_at TIMESTAMPTZ NOT NULL
);
