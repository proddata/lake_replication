\set ON_ERROR_STOP on
\connect application
CREATE TABLE public.orders (
    id bigint PRIMARY KEY,
    customer text NOT NULL,
    item text NOT NULL,
    amount double precision NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO public.orders (id, customer, item, amount) VALUES
    (1, 'Ada Lovelace', 'analytical engine', 1200.00),
    (2, 'Grace Hopper', 'compiler', 340.50),
    (3, 'Katherine Johnson', 'trajectory chart', 89.99);
CREATE PUBLICATION orders_pub FOR TABLE public.orders;
