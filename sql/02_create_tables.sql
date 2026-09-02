CREATE TABLE IF NOT EXISTS ecommerce.customers (
    customer_id BIGSERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(30),
    country VARCHAR(100),
    state VARCHAR(100),
    city VARCHAR(100),
    signup_date TIMESTAMP NOT NULL,
    customer_segment VARCHAR(50),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ecommerce.products (
    product_id BIGSERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    price NUMERIC(12,2) NOT NULL,
    cost NUMERIC(12,2) NOT NULL,
    supplier_id BIGINT,
    inventory_quantity INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ecommerce.orders (
    order_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    order_date TIMESTAMP NOT NULL,
    status VARCHAR(50) NOT NULL,
    payment_method VARCHAR(50),
    shipping_country VARCHAR(100),
    shipping_state VARCHAR(100),
    total_amount NUMERIC(12,2) NOT NULL,
    discount NUMERIC(12,2) DEFAULT 0,
    tax NUMERIC(12,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id)
        REFERENCES ecommerce.customers(customer_id)
);

CREATE TABLE IF NOT EXISTS ecommerce.order_items (
    order_item_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(12,2) NOT NULL,
    discount NUMERIC(12,2) DEFAULT 0,

    CONSTRAINT fk_order_items_order
        FOREIGN KEY (order_id)
        REFERENCES ecommerce.orders(order_id),

    CONSTRAINT fk_order_items_product
        FOREIGN KEY (product_id)
        REFERENCES ecommerce.products(product_id)
);

CREATE TABLE IF NOT EXISTS ecommerce.payments (
    payment_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    payment_method VARCHAR(50),
    payment_status VARCHAR(50),
    transaction_timestamp TIMESTAMP NOT NULL,

    CONSTRAINT fk_payments_order
        FOREIGN KEY (order_id)
        REFERENCES ecommerce.orders(order_id)
);

CREATE TABLE IF NOT EXISTS ecommerce.inventory (
    inventory_id BIGSERIAL PRIMARY KEY,
    product_id BIGINT NOT NULL,
    warehouse_id BIGINT NOT NULL,
    quantity INTEGER NOT NULL,
    reserved_quantity INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_inventory_product
        FOREIGN KEY (product_id)
        REFERENCES ecommerce.products(product_id)
);

CREATE TABLE IF NOT EXISTS ecommerce.website_events (
    event_id UUID PRIMARY KEY,
    customer_id BIGINT,
    session_id UUID NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    product_id BIGINT,
    event_timestamp TIMESTAMP NOT NULL,
    device VARCHAR(50),
    browser VARCHAR(100),
    ip_address INET
);

CREATE TABLE IF NOT EXISTS ecommerce.support_tickets (
    ticket_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    category VARCHAR(100),
    priority VARCHAR(50),
    message TEXT,
    status VARCHAR(50),
    resolution_time_minutes INTEGER,

    CONSTRAINT fk_support_customer
        FOREIGN KEY (customer_id)
        REFERENCES ecommerce.customers(customer_id)
);

CREATE TABLE IF NOT EXISTS ecommerce.marketing_events (
    marketing_event_id BIGSERIAL PRIMARY KEY,
    campaign_id BIGINT NOT NULL,
    customer_id BIGINT,
    campaign VARCHAR(255),
    channel VARCHAR(100),
    impression BOOLEAN DEFAULT FALSE,
    click BOOLEAN DEFAULT FALSE,
    conversion BOOLEAN DEFAULT FALSE,
    cost NUMERIC(12,2) DEFAULT 0,
    event_timestamp TIMESTAMP NOT NULL,

    CONSTRAINT fk_marketing_customer
        FOREIGN KEY (customer_id)
        REFERENCES ecommerce.customers(customer_id)
);