package io.jdbd.mysql.protocol.client;

interface ProtocolConstants {


    /**
     * {@code enum_resultset_metadata} No metadata will be sent.
     *
     * @see #RESULTSET_METADATA_FULL
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#aba06d1157f6dee3f20537154103c91a1">enum_resultset_metadata</a>
     */
    int RESULTSET_METADATA_NONE = 0;
    /**
     * {@code enum_resultset_metadata} The server will send all metadata.
     *
     * @see #RESULTSET_METADATA_NONE
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#aba06d1157f6dee3f20537154103c91a1">enum_resultset_metadata</a>
     */
    int RESULTSET_METADATA_FULL = 1;

    // below enum_cursor_type @see https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#a3e5e9e744ff6f7b989a604fd669977da
    int CURSOR_TYPE_NO_CURSOR = 0;
    int CURSOR_TYPE_READ_ONLY = 1;

    //below  Protocol field type numbers, see https://dev.mysql.com/doc/dev/mysql-server/latest/field__types_8h.html#a69e798807026a0f7e12b1d6c72374854
    //
    int TYPE_DECIMAL = 0;
    int TYPE_TINY = 1;
    int TYPE_SHORT = 2;
    int TYPE_LONG = 3;

    int TYPE_FLOAT = 4;
    int TYPE_DOUBLE = 5;
    int TYPE_NULL = 6;
    int TYPE_TIMESTAMP = 7;

    int TYPE_LONGLONG = 8;
    int TYPE_INT24 = 9;
    int TYPE_DATE = 10;
    int TYPE_TIME = 11;

    int TYPE_DATETIME = 12;
    int TYPE_YEAR = 13;
    int TYPE_VARCHAR = 15;
    int TYPE_BIT = 16;

    int TYPE_BOOL = 244;
    int TYPE_JSON = 245;
    int TYPE_ENUM = 247;
    int TYPE_SET = 248;

    int TYPE_TINY_BLOB = 249;
    int TYPE_MEDIUM_BLOB = 250;
    int TYPE_LONG_BLOB = 251;
    int TYPE_BLOB = 252;

    int TYPE_VAR_STRING = 253;
    int TYPE_STRING = 254;
    int TYPE_GEOMETRY = 255;
    int TYPE_NEWDECIMAL = 246;
}
