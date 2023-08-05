package io.jdbd.postgre.protocol.client;


import io.jdbd.session.Option;

public interface PgErrorOrNotice {

    /**
     * below postgre Error and Notice Message Fields
     *
     * @see <a href="https://www.postgresql.org/docs/current/protocol-error-fields.html">Error and Notice Message Fields</a>
     */
    Option<String> PG_MSG_SEVERITY_S = Option.from("PG_MSG_FIELD_SEVERITY_S", String.class);
    Option<String> PG_MSG_SEVERITY_V = Option.from("PG_MSG_FIELD_SEVERITY_V", String.class);
    Option<String> PG_MSG_SQLSTATE = Option.from("PG_MSG_FIELD_SQLSTATE", String.class);
    Option<String> PG_MSG_MESSAGE = Option.from("PG_MSG_FIELD_MESSAGE", String.class);

    Option<String> PG_MSG_DETAIL = Option.from("PG_MSG_FIELD_DETAIL", String.class);
    Option<String> PG_MSG_HINT = Option.from("PG_MSG_FIELD_HINT", String.class);
    Option<String> PG_MSG_POSITION = Option.from("PG_MSG_FIELD_POSITION", String.class);
    Option<String> PG_MSG_INTERNAL_POSITION = Option.from("PG_MSG_FIELD_INTERNAL_POSITION", String.class);

    Option<String> PG_MSG_INTERNAL_QUERY = Option.from("PG_MSG_FIELD_INTERNAL_QUERY", String.class);
    Option<String> PG_MSG_WHERE = Option.from("PG_MSG_FIELD_WHERE", String.class);
    Option<String> PG_MSG_SCHEMA = Option.from("PG_MSG_FIELD_SCHEMA", String.class);
    Option<String> PG_MSG_TABLE = Option.from("PG_MSG_FIELD_TABLE", String.class);

    Option<String> PG_MSG_COLUMN = Option.from("PG_MSG_FIELD_COLUMN", String.class);
    Option<String> PG_MSG_DATATYPE = Option.from("PG_MSG_FIELD_DATATYPE", String.class);
    Option<String> PG_MSG_CONSTRAINT = Option.from("PG_MSG_FIELD_CONSTRAINT", String.class);
    Option<String> PG_MSG_FILE = Option.from("PG_MSG_FIELD_FILE", String.class);

    Option<String> PG_MSG_LINE = Option.from("PG_MSG_FIELD_LINE", String.class);
    Option<String> PG_MSG_ROUTINE = Option.from("PG_MSG_FIELD_ROUTINE", String.class);


}
