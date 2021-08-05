package io.jdbd.postgre.protocol.client;

/**
 * Known server parameters status.
 *
 * @see <a href="https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-ASYNC">Asynchronous Operations</a>
 */
enum ServerStatus {

    server_version, // cannot change after startup
    client_encoding,
    is_superuser,
    session_authorization,
    DateStyle,
    // below after Postgre server 8.0 .
    server_encoding, // cannot change after startup
    TimeZone,
    integer_datetimes, // cannot change after startup
    // below after Postgre server 8.1 .
    standard_conforming_strings,
    // below after Postgre server 8.4 .
    IntervalStyle,
    // below after Postgre server 9.0 .
    application_name


}
