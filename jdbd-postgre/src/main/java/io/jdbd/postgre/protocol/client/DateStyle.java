package io.jdbd.postgre.protocol.client;

/**
 * <p>
 * date/time format
 * </p>
 *
 * @see DateOrder
 * @see <a href="https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-DATETIME-OUTPUT">Date/Time Output</a>
 * @see <a href="https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-DATESTYLE">DateStyle (string)</a>
 * @since 1.0
 */
enum DateStyle {

    ISO,
    SQL,
    Postgres,
    German

}
