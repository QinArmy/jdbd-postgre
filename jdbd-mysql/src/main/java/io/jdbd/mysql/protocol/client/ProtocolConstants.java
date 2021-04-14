package io.jdbd.mysql.protocol.client;

public interface ProtocolConstants {


    /**
     * {@code enum_resultset_metadata} No metadata will be sent.
     *
     * @see #RESULTSET_METADATA_FULL
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#aba06d1157f6dee3f20537154103c91a1">enum_resultset_metadata</a>
     */
    byte RESULTSET_METADATA_NONE = 0;
    /**
     * {@code enum_resultset_metadata} The server will send all metadata.
     *
     * @see #RESULTSET_METADATA_NONE
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#aba06d1157f6dee3f20537154103c91a1">enum_resultset_metadata</a>
     */
    byte RESULTSET_METADATA_FULL = 1;

    // below enum_cursor_type @see https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#a3e5e9e744ff6f7b989a604fd669977da
    byte CURSOR_TYPE_NO_CURSOR = 0;
    byte CURSOR_TYPE_READ_ONLY = 1;

    //below  Protocol field type numbers, see https://dev.mysql.com/doc/dev/mysql-server/latest/field__types_8h.html#a69e798807026a0f7e12b1d6c72374854
    //
    byte TYPE_DECIMAL = 0;
    byte TYPE_TINY = 1;
    byte TYPE_SHORT = 2;
    byte TYPE_LONG = 3;

    byte TYPE_FLOAT = 4;
    byte TYPE_DOUBLE = 5;
    byte TYPE_NULL = 6;
    byte TYPE_TIMESTAMP = 7;

    byte TYPE_LONGLONG = 8;
    byte TYPE_INT24 = 9;
    byte TYPE_DATE = 10;
    byte TYPE_TIME = 11;

    byte TYPE_DATETIME = 12;
    byte TYPE_YEAR = 13;
    byte TYPE_VARCHAR = 15;
    byte TYPE_BIT = 16;

    short TYPE_BOOL = 244;
    short TYPE_JSON = 245;
    short TYPE_ENUM = 247;
    short TYPE_SET = 248;

    short TYPE_TINY_BLOB = 249;
    short TYPE_MEDIUM_BLOB = 250;
    short TYPE_LONG_BLOB = 251;
    short TYPE_BLOB = 252;

    short TYPE_VAR_STRING = 253;
    short TYPE_STRING = 254;
    short TYPE_GEOMETRY = 255;
    short TYPE_NEWDECIMAL = 246;
}
