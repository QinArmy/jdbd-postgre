CREATE TABLE army.my_types (
    id              bigserial                                                  NOT NULL
        CONSTRAINT my_types_pk
            PRIMARY KEY,
    my_boolean      boolean                DEFAULT FALSE                       NOT NULL,
    create_time     timestamp              DEFAULT NOW()                       NOT NULL,
    my_time0        time(2)                DEFAULT NOW()                       NOT NULL,
    my_zoned_time1  time(1) WITH TIME ZONE DEFAULT NOW(),
    my_varbit       bit varying(64)        DEFAULT '0'::"bit",
    my_bytea        bytea                  DEFAULT '\x00'::bytea               NOT NULL,
    my_json         json                   DEFAULT '{}'                        NOT NULL,
    my_jsonb        jsonb                  DEFAULT '{}'                        NOT NULL,
    my_xml          xml                    DEFAULT '
    <sql></sql>'                                                               NOT NULL,
    NOT                                                                        NULL,
    my_money        money                  DEFAULT 0                           NOT NULL,
    my_point        point                  DEFAULT '(0,0)'::point              NOT NULL,
    my_line         line                   DEFAULT '{1,-1,0}'::line            NOT NULL,
    my_gender       army.gender            DEFAULT 'M'                         NOT NULL,
    my_line_segment lseg                   DEFAULT '[ ( 0 , 0 ) , ( 1 , 1 ) ]' NOT NULL,
    my_int4_range   int4range              DEFAULT 'empty'::int4range          NOT NULL,
    my_path         path                   DEFAULT '[(0,0),(1,1)]'::path       NOT NULL,
    my_decimal      numeric(14, 2)         DEFAULT 0.0                         NOT NULL,
    my_interval     interval               DEFAULT '00:00:00'::interval        NOT NULL,
    my_smallint     smallint               DEFAULT 0                           NOT NULL
)
