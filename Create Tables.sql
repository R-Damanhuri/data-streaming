CREATE TABLE IF NOT EXISTS transactions(
    transaction_id VARCHAR(255) PRIMARY KEY,
    product_id VARCHAR(255),
    product_name VARCHAR(255),
    product_category VARCHAR(255),
    product_price DOUBLE PRECISION,
    product_quantity INTEGER,
    product_brand VARCHAR(255),
    customer_id VARCHAR(255),
    transaction_date TIMESTAMP,
    payment_method VARCHAR(255),
    total_amount DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS sales_per_category(
    transaction_date TIMESTAMP,
    category VARCHAR(255),
    total_sales DOUBLE PRECISION,
    PRIMARY KEY (transaction_date, category)
);

CREATE TABLE IF NOT EXISTS sales_per_day(
    transaction_date DATE PRIMARY KEY,
    total_sales DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS sales_per_month(
    year INTEGER,
    month INTEGER,
    total_sales DOUBLE PRECISION,
    PRIMARY KEY (year, month)
);