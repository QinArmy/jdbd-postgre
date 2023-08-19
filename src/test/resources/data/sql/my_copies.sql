CREATE TABLE army.my_copies (
    id          bigserial                                 NOT NULL PRIMARY KEY,
    create_time timestamp   DEFAULT NOW()                 NOT NULL,
    my_varchar  varchar(65) DEFAULT ''::character varying NOT NULL
)
