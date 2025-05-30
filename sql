-- Table: public.gnss_data

-- DROP TABLE IF EXISTS public.gnss_data;

CREATE TABLE IF NOT EXISTS public.gnss_data
(
    id integer NOT NULL DEFAULT nextval('gnss_data_id_seq'::regclass),
    "timestamp" timestamp without time zone NOT NULL,
    latitude double precision NOT NULL,
    longitude double precision NOT NULL,
    altitude double precision,
    speed double precision,
    CONSTRAINT gnss_data_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.gnss_data
    OWNER to "r.giessler";