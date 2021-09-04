package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.BatchBindStmt;
import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgNumbers;
import io.jdbd.vendor.stmt.Stmt;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

final class ExtendedCommandWriter {


    static Mono<Iterable<ByteBuf>> write(Stmt stmt, ExtendedStmtTask stmtTask) {
        return Mono.empty();
    }

    private final String statementName;

    private final TaskAdjutant adjutant;

    private final ExtendedStmtTask stmtTask;

    private final ByteBuf message;

    private final List<List<BindValue>> paramGroupList;

    private final int fetchSize;

    private int groupIndex;

    private int paramIndex;

    private ExtendedCommandWriter(final List<List<BindValue>> paramGroupList, final ExtendedStmtTask stmtTask) {
        this.statementName = stmtTask.getStatementName();
        this.adjutant = stmtTask.adjutant();
        this.stmtTask = stmtTask;
        this.paramGroupList = paramGroupList;

        this.fetchSize = stmtTask.getFetchSize();
        this.message = this.adjutant.allocator().buffer(1024, Integer.MAX_VALUE);


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

        final int groupIndex = this.groupIndex;
        final List<BindValue> paramGroup = this.paramGroupList.get(groupIndex);
        final int paramCount = paramGroup.size();
        message.writeShort(paramCount); // The number of parameter format codes
        for (BindValue bindValue : paramGroup) {
            message.writeShort(decideParameterFormatCode(bindValue.getType()));
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
        final List<BindValue> paramGroup = this.paramGroupList.get(this.groupIndex);
        final int paramCount = paramGroup.size();

        final ByteBuf message = this.message;

        try {
            for (int i = this.paramIndex; i < paramCount; i++) {
                final BindValue bindValue = paramGroup.get(i);
                final Object value = bindValue.getValue();
                if (value == null) {
                    message.writeInt(-1); //  -1 indicates a NULL parameter value
                    continue;
                }
                if (value instanceof Publisher) {
                    continue;
                } else {
                    writeBindValue(bindValue);
                }
            }
        } catch (SQLException e) {

        }

    }

    private void writeBindValue(BindValue bindValue) throws SQLException {
        switch (bindValue.getType()) {
            case SMALLINT:
                bindToSmallInt(bindValue);
                break;
            case INTEGER:
            case REAL:
            case DOUBLE:
            case DECIMAL:
            case BIGINT:

            case VARCHAR:
            case BIT:
            case VARBIT:
            case MONEY:

            case BYTEA:

            case OID:
            case TEXT:
            case CHAR:
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
                throw PgExceptions.createUnexpectedEnumException(bindValue.getType());
        }
    }

    /**
     * @see #writeBindValue(BindValue)
     */
    private void bindToSmallInt(BindValue bindValue) throws SQLException {
        final Object nonNull = bindValue.getNonNullValue();

        final short smallInt;
        if (nonNull instanceof Number) {
            smallInt = PgNumbers.convertNumberToShort((Number) nonNull);
        } else if (nonNull instanceof String) {
            smallInt = Short.parseShort((String) nonNull);
        } else {
            throw PgExceptions.createNotSupportBindTypeError(this.groupIndex, bindValue);
        }
        this.message.writeShort(smallInt);
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
