-- Drop table

-- DROP TABLE crypto.fundings;

CREATE TABLE crypto.fundings (
	id serial NOT NULL,
	"date" timestamp(0) NOT NULL,
	asset varchar(20) NOT NULL,
	future varchar(20) NOT NULL,
	"type" varchar NOT NULL,
	exchange varchar NOT NULL,
	rate numeric NULL,
	meta json NULL,
	updated_at timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
	created_at timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT fundings_pk PRIMARY KEY (id),
	CONSTRAINT fundings_un UNIQUE (date, asset, future, type, exchange)
);
