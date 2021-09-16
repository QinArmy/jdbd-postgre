package io.jdbd.postgre;

/**
 * Known server parameters status.
 *
 * @see <a href="https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-ASYNC">Asynchronous Operations</a>
 */
public enum ServerParameter {

    server_version, // cannot change after startup
    client_encoding,
    is_superuser,
    session_authorization,
    backslash_quote,
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
    application_name,

    // below get by 'SHOW ALL' command
    lc_monetary;


    public static boolean isOn(String boolValue) {
        final boolean on;
        switch (boolValue) {
            case "on":
            case "1":
            case "true":
                on = true;
                break;
            case "off":
            case "false":
            case "0":
                on = false;
                break;
            default:
                throw new IllegalArgumentException(String.format("Unknown bool value[%s]", boolValue));

        }
        return on;
    }


}
