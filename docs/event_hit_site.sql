-- ============================================================
-- space_bloom — Web Analytics Hit Table
-- DDL for PostgreSQL
-- ============================================================

CREATE TABLE IF NOT EXISTS space_bloom.hits (

    hit_time_gmt            BIGINT
        CONSTRAINT hits_hit_time_gmt_nn NOT NULL,
    date_time               TIMESTAMP WITHOUT TIME ZONE
        CONSTRAINT hits_date_time_nn NOT NULL,
    user_agent              TEXT,
    ip                      VARCHAR(20),
    geo_city                VARCHAR(32),
    geo_region              VARCHAR(32),
    geo_country             VARCHAR(4),
    event_list              TEXT,
    pagename                VARCHAR(100),
    page_url                VARCHAR(255),
    referrer                VARCHAR(255),
    product_list_raw        TEXT,
    product_category        VARCHAR(255),
    product_name            VARCHAR(255),
    product_num_items       SMALLINT,
    product_revenue         NUMERIC(12, 2),
    product_custom_events   VARCHAR(512),
    product_merch_evars     VARCHAR(512),
    inserted_at             TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()

);
-- ============================================================
-- Indexes
-- ============================================================

-- Primary lookup by time
CREATE INDEX IF NOT EXISTS idx_hits_date_time
    ON space_bloom.hits (date_time);

-- IP-based visitor session grouping
CREATE INDEX IF NOT EXISTS idx_hits_ip
    ON space_bloom.hits (ip);

-- Page-level reporting
CREATE INDEX IF NOT EXISTS idx_hits_pagename
    ON space_bloom.hits (pagename);

-- Product-level reporting
CREATE INDEX IF NOT EXISTS idx_hits_product_name
    ON space_bloom.hits (product_name);

-- Event filtering (e.g. purchase events only)
CREATE INDEX IF NOT EXISTS idx_hits_event_list
    ON space_bloom.hits USING gin (string_to_array(event_list, ','));

-- ============================================================
-- Table & Column Comments
-- ============================================================

COMMENT ON TABLE space_bloom.hits IS
    'Web analytics hit-level data for space_bloom. Each row represents a '
    'single server call (page view or event) recorded by the collection layer. '
    'Product data is both stored raw (product_list_raw) and parsed into '
    'discrete columns for reporting convenience.';

COMMENT ON COLUMN space_bloom.hits.hit_time_gmt IS
    'The timestamp of the hit based in Unix time.';

COMMENT ON COLUMN space_bloom.hits.date_time IS
    'The time of the hit in readable format, based on the report suite''s time zone.';

COMMENT ON COLUMN space_bloom.hits.user_agent IS
    'User agent string sent in the HTTP header of the image request.';

COMMENT ON COLUMN space_bloom.hits.ip IS
    'IP Address based on the HTTP header of the image request.';

COMMENT ON COLUMN space_bloom.hits.geo_city IS
    'Name of the city the hit came from, based on IP.';

COMMENT ON COLUMN space_bloom.hits.geo_region IS
    'Name of the state or region the hit came from, based on IP.';

COMMENT ON COLUMN space_bloom.hits.geo_country IS
    'Abbreviation of the country the hit came from, based on IP.';

COMMENT ON COLUMN space_bloom.hits.event_list IS
    'A comma separated list of events that occur during the visit. Example format: "2,200,201,100"';

COMMENT ON COLUMN space_bloom.hits.pagename IS
    'Used to populate the Pages dimension. If the pagename variable is empty, Analytics uses page_url instead.';

COMMENT ON COLUMN space_bloom.hits.page_url IS
    'The URL of the hit. Not used in link tracking image requests.';

COMMENT ON COLUMN space_bloom.hits.referrer IS
    'The referring URL to the page the visitor is currently reviewing. '
    'Example: http://search.yahoo.com/search?p=marketing&sm=Yahoo%21+Search';

COMMENT ON COLUMN space_bloom.hits.product_list_raw IS
    'Product list as passed in through the products variable. Products are delimited by commas '
    'while individual product properties are delimited by semicolons. '
    'For details on how the product_list is formatted, please review Appendix B.';

COMMENT ON COLUMN space_bloom.hits.product_category IS
    'The category for the product (e.g. Shoes, Clothes). Parsed from product_list_raw position 1.';

COMMENT ON COLUMN space_bloom.hits.product_name IS
    'Either the product ID or the product name. Parsed from product_list_raw position 2.';

COMMENT ON COLUMN space_bloom.hits.product_num_items IS
    'The number of products. Parsed from product_list_raw position 3.';

COMMENT ON COLUMN space_bloom.hits.product_revenue IS
    'The price of the product. Revenue is only actualized when the purchase event is set in event_list. '
    'Parsed from product_list_raw position 4.';

COMMENT ON COLUMN space_bloom.hits.product_custom_events IS
    'Events only applied to a specific product, pipe-delimited. Parsed from product_list_raw position 5.';

COMMENT ON COLUMN space_bloom.hits.product_merch_evars IS
    'eVars only applied to a specific product, pipe-delimited. Parsed from product_list_raw position 6.';

COMMENT ON COLUMN space_bloom.hits.inserted_at IS
    'UTC timestamp when this row was loaded into the table.';
