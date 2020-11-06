package io.jdbd.mysql.protocol.client;

import java.nio.charset.Charset;

final class ColumnMeta {

    private final String alias;

    private final Charset charset;

    private final MySQLType type;

    private final short definitionFlags;

    private final byte decimals;

    ColumnMeta(String alias, Charset charset, MySQLType type, short definitionFlags, byte decimals) {
        this.alias = alias;
        this.charset = charset;
        this.type = type;
        this.definitionFlags = definitionFlags;
        this.decimals = decimals;
    }
}
