package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.BatchBindStmt;
import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.util.PgBinds;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.stmt.Stmt;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

final class ExtendedBindWriter {


    static Mono<Iterable<ByteBuf>> write(Stmt stmt, ExtendedStmtTask stmtTask) {
        return Mono.empty();
    }

    private final String statementName;

    private final TaskAdjutant adjutant;

    private final ExtendedStmtTask stmtTask;

    private final ByteBuf message;

    private final List<PgType> paramTypeList;

    private final int batchIndex;

    private final List<? extends ParamValue> bindGroup;

    private final Charset clientCharset;

    private final int fetchSize;

    private int paramIndex = 0;

    private ExtendedBindWriter(final int batchIndex, final List<? extends ParamValue> bindGroup
            , final ExtendedStmtTask stmtTask) {
        this.batchIndex = batchIndex;
        this.statementName = stmtTask.getStatementName();
        this.adjutant = stmtTask.adjutant();
        this.stmtTask = stmtTask;

        this.bindGroup = bindGroup;
        this.paramTypeList = stmtTask.getParamTypeList();

        // if execute batch ,then can't use fetch
        this.fetchSize = batchIndex == 0 ? stmtTask.getFetchSize() : 0;
        this.message = this.adjutant.allocator().buffer(1024, Integer.MAX_VALUE);
        this.clientCharset = this.adjutant.clientCharset();

    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Bind</a>
     */
    private void writeCommand() {
        final ByteBuf message = this.message;
        message.writeByte(Messages.B);
        message.writeZero(Messages.LENGTH_BYTES);//placeholder of length
        // The name of the destination portal (an empty string selects the unnamed portal).
        final String portalName = this.stmtTask.getNewPortalName();
        if (portalName != null) {
            message.writeBytes(portalName.getBytes(this.adjutant.clientCharset()));
        }
        message.writeByte(Messages.STRING_TERMINATOR);
        // The name of the source prepared statement (an empty string selects the unnamed prepared statement).
        final String statementName = this.statementName;
        if (statementName != null) {
            message.writeBytes(statementName.getBytes(this.adjutant.clientCharset()));
        }
        message.writeByte(Messages.STRING_TERMINATOR);

        final List<PgType> paramTypeList = this.paramTypeList;
        final int paramCount = paramTypeList.size();

        if (this.bindGroup.size() != paramCount) {
            throw PgExceptions.parameterCountMatch(this.batchIndex, paramCount, this.bindGroup.size());
        }
        message.writeShort(paramCount); // The number of parameter format codes
        for (PgType type : paramTypeList) {
            message.writeShort(decideParameterFormatCode(type));
        }
        message.writeShort(paramCount); // The number of parameter values
        this.paramIndex = 0;
        if (paramCount > 0) {
            continueWriteBindParam();
        }

    }

    /**
     * @see #writeCommand()
     */
    private int decideParameterFormatCode(final PgType type) {
        final int formatCode;
        switch (type) {
            case SMALLINT:
            case INTEGER:
            case REAL:
            case DOUBLE:
            case BIGINT:
            case BYTEA:
                formatCode = 1; // binary format code
                break;
            default:
                formatCode = 0; // all array type is text format
        }
        return formatCode;
    }


    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Bind</a>
     */
    private void continueWriteBindParam() {
        final List<PgType> paramTypeList = this.paramTypeList;
        final int paramCount = paramTypeList.size();
        final List<? extends ParamValue> bindGroup = this.bindGroup;

        final ByteBuf message = this.message;
        try {
            for (int i = this.paramIndex, valueLengthIndex, valueLength, valueEndIndex; i < paramCount; i++) {
                final ParamValue paramValue = bindGroup.get(i);
                final Object value = paramValue.getValue();
                if (value == null) {
                    message.writeInt(-1); //  -1 indicates a NULL parameter value
                    continue;
                }
                if (value instanceof Publisher) {
                    continue;
                } else {
                    valueLengthIndex = message.writerIndex();
                    message.writeZero(4); // placeholder of parameter value length.

                    writeNonNullBindValue(paramTypeList.get(i), paramValue);
                    valueEndIndex = message.writerIndex();

                    message.writerIndex(valueLengthIndex);
                    message.writeInt(valueEndIndex - valueLengthIndex - 4);
                    message.writerIndex(valueEndIndex);
                }
            }
        } catch (SQLException e) {

        }

    }

    private void writeNonNullBindValue(final PgType pgType, final ParamValue paramValue) throws SQLException {
        switch (pgType) {
            case SMALLINT: {
                this.message.writeShort(PgBinds.bindNonNullToShort(this.batchIndex, pgType, paramValue));
            }
            break;
            case INTEGER: {
                this.message.writeInt(PgBinds.bindNonNullToInt(this.batchIndex, pgType, paramValue));
            }
            break;
            case OID:
            case BIGINT: {
                this.message.writeLong(PgBinds.bindNonNullToLong(this.batchIndex, pgType, paramValue));
            }
            break;
            case REAL: {
                final float value = PgBinds.bindNonNullToFloat(this.batchIndex, pgType, paramValue);
                this.message.writeInt(Float.floatToIntBits(value));
            }
            break;
            case DOUBLE: {
                final double value = PgBinds.bindNonNullToDouble(this.batchIndex, pgType, paramValue);
                this.message.writeLong(Double.doubleToLongBits(value));
            }
            break;
            case DECIMAL: {
                final String text = PgBinds.bindNonNullToDecimal(this.batchIndex, pgType, paramValue)
                        .toPlainString();
                Messages.writeString(this.message, text, this.clientCharset);
            }
            break;
            case TEXT:
            case CHAR:
            case MONEY:
            case VARCHAR: {
                final String text = PgBinds.bindNonNullToString(this.batchIndex, pgType, paramValue);
                Messages.writeString(this.message, text, this.clientCharset);
            }
            case BIT:
            case VARBIT: {
                final String bitString = PgBinds.bindNonNullToBit(this.batchIndex, pgType, paramValue);
                Messages.writeString(this.message, bitString, this.clientCharset);
            }
            break;
            case BYTEA: {
                bindNonNullToBytea(pgType, paramValue);
            }
            break;
            case TSQUERY:
            case TSVECTOR:

            case TIME:
            case TIMETZ:
            case DATE:
            case TIMESTAMP:
            case TIMESTAMPTZ:
            case INTERVAL:

            case MACADDR8:
            case MACADDR:
            case INET:
            case CIDR:


            case BOOLEAN:
            case JSON:
            case JSONB:
            case UUID:
            case XML:

            case NUMRANGE:
            case DATERANGE:
            case TSRANGE:
            case INT4RANGE:
            case INT8RANGE:
            case TSTZRANGE: // all range type is text format

            case POINT:
            case CIRCLE:
            case LINE:
            case PATH:
            case POLYGON:
            case LINE_SEGMENT:
            case BOX:// all geometry type is text format

            case BOX_ARRAY:
            case BIGINT_ARRAY:
            case POINT_ARRAY:
            case MONEY_ARRAY:
            case JSONB_ARRAY:
            case BYTEA_ARRAY:
            case UUID_ARRAY:
            case OID_ARRAY:
            case BIT_ARRAY:
            case TIME_ARRAY:
            case JSON_ARRAY:
            case DATE_ARRAY:
            case CHAR_ARRAY:
            case XML_ARRAY:
            case CIDR_ARRAY:
            case DOUBLE_ARRAY:
            case INET_ARRAY:
            case LINE_ARRAY:
            case PATH_ARRAY:
            case REAL_ARRAY:
            case TEXT_ARRAY:
            case BOOLEAN_ARRAY:
            case CIRCLES_ARRAY:
            case MACADDR_ARRAY:
            case POLYGON_ARRAY:
            case TSQUERY_ARRAY:
            case TSRANGE_ARRAY:
            case MACADDR8_ARRAY:
            case NUMRANGE_ARRAY:
            case TSVECTOR_ARRAY:
            case DATERANGE_ARRAY:
            case LINE_SEGMENT_ARRAY:
            case INT4RANGE_ARRAY:
            case INT8RANGE_ARRAY:
            case TSTZRANGE_ARRAY:
            case TIMESTAMPTZ_ARRAY:
            case REF_CURSOR_ARRAY:
            case TIMESTAMP_ARRAY:
            case SMALLINT_ARRAY:
            case INTERVAL_ARRAY:
            case VARCHAR_ARRAY:
            case INTEGER_ARRAY:
            case DECIMAL_ARRAY:
            case VARBIT_ARRAY:
            case TIMETZ_ARRAY:


                break;
            case UNSPECIFIED:
            case REF_CURSOR:
            default:
                throw PgExceptions.createUnexpectedEnumException(pgType);
        }
    }

    /**
     * @see #writeNonNullBindValue(PgType, ParamValue)
     */
    private void bindNonNullToBytea(PgType pgType, ParamValue paramValue) throws SQLException {
        final Object nonNull = paramValue.getNonNullValue();
        final byte[] value;
        if (nonNull instanceof byte[]) {
            value = (byte[]) nonNull;
        } else if (nonNull instanceof BigDecimal) {
            value = ((BigDecimal) nonNull).toPlainString().getBytes(this.clientCharset);
        } else {
            value = nonNull.toString().getBytes(this.clientCharset);
        }
        this.message.writeBytes(value);
    }


    private static List<List<BindValue>> obtainParamGroupList(final Stmt stmt) {
        final List<List<BindValue>> groupList;
        if (stmt instanceof BindStmt) {
            groupList = Collections.singletonList(((BindStmt) stmt).getParamGroup());
        } else if (stmt instanceof BatchBindStmt) {
            groupList = ((BatchBindStmt) stmt).getGroupList();
        } else {
            throw new IllegalArgumentException(String.format("Unsupported Stmt[%s]", stmt.getClass().getName()));
        }
        return groupList;
    }


}
