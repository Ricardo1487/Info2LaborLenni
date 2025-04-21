CREATE TABLE gps_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    altitude DOUBLE PRECISION
);