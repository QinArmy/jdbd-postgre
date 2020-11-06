package io.jdbd.mysql.protocol.client;

import io.jdbd.meta.SQLType;

import java.util.HashSet;
import java.util.Set;

public enum MySQLType implements SQLType {

    DECIMAL(0),
    TINY(1),
    SHORT(2),
    LONG(3),

    FLOAT(4),
    DOUBLE(5),
    NULL(6),
    TIMESTAMP(7),

    LONGLONG(8),
    INT24(9),
    DATE(10),
    TIME(11),

    DATETIME(12),
    YEAR(13),
    NEWDATE(0, false), //Internal to MySQL.Not used in protocol
    VARCHAR(15),

    BIT(16),
    TIMESTAMP2(17),
    DATETIME2(18, false),//Internal to MySQL.Not used in protocol
    TIME2(19, false),//Internal to MySQL.Not used in protocol

    TYPED_ARRAY(20, false), //Used for replication only.
    INVALID(243),
    BOOL(244, false), //Currently just a placeholder.
    JSON(245),

    NEWDECIMAL(246),
    ENUM(247),
    SET(248),
    TINY_BLOB(249),

    MEDIUM_BLOB(250),
    LONG_BLOB(251),
    BLOB(252),
    VAR_STRING(253),

    STRING(254),
    GEOMETRY(255);

    static {
        Set<Integer> codeSet = new HashSet<>();
        for (MySQLType value : values()) {
            if (!codeSet.add(value.code)) {
                throw new IllegalStateException(String.format("MySQLType[%s] code duplication.", value.name()));
            }
        }
    }

    private final int code;

    private final boolean useInProtocol;

    MySQLType(int code) {
        this(code, true);
    }

    MySQLType(int code, boolean useInProtocol) {
        this.code = code;
        this.useInProtocol = useInProtocol;
    }

    @Override
    public int code() {
        return this.code;
    }

    @Override
    public boolean useInProtocol() {
        return this.useInProtocol;
    }


}
