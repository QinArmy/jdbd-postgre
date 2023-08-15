package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.syntax.PgStatement;
import io.jdbd.postgre.util.PgBinds;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.postgre.util.PgTimes;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.vendor.stmt.*;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @see ExtendedQueryTask
 */
final class PgExtendedCommandWriter extends CommandWriter implements ExtendedCommandWriter {

    static PgExtendedCommandWriter create(ExtendedStmtTask stmtTask) throws SQLException {
        return new PgExtendedCommandWriter(stmtTask);
    }

    private static final Logger LOG = LoggerFactory.getLogger(PgExtendedCommandWriter.class);

    private final ExtendedStmtTask stmtTask;

    private final ParamSingleStmt stmt;

    private final boolean oneRoundTrip;

    private final String statementName;

    private final PostgreStmt cacheStmt;

    private List<PgType> paramTypeList;

    /**
     * if support fetch ,then create portal name.
     */
    private final String portalName;

    private int fetchSize;


    private PgExtendedCommandWriter(final ExtendedStmtTask stmtTask) {
        super(stmtTask.adjutant());
        this.stmtTask = stmtTask;
        this.stmt = stmtTask.getStmt();
        this.oneRoundTrip = isOneShotStmt(this.stmt);

        final String sql = this.stmt.getSql();
        final PostgreStmt cacheStmt;
        cacheStmt = this.adjutant.parseAsPostgreStmt(sql);
        assert cacheStmt.originalSql().equals(sql);
        this.cacheStmt = cacheStmt;
        if (cacheStmt instanceof ServerCacheStmt) {
            this.statementName = ((ServerCacheStmt) cacheStmt).stmtName();
        } else {
            this.statementName = this.adjutant.nextStmtName();
        }

        if (this.oneRoundTrip
                || (cacheStmt instanceof ServerCacheStmt && ((ServerCacheStmt) cacheStmt).getRowMeta() == null)) {
            this.portalName = "";
        } else {
            this.portalName = this.adjutant.nextPortName(this.statementName);
        }


    }


    @Override
    public boolean isOneRoundTrip() {
        return this.oneRoundTrip;
    }

    @Override
    public boolean isNeedPrepare() {
        return this.stmt instanceof PrepareStmt && !(this.cacheStmt instanceof ServerCacheStmt);
    }

    @Override
    public boolean supportFetch() {
        return this.fetchSize > 0 && PgStrings.hasText(this.portalName);
    }

    @Override
    public boolean isNeedClose() {
        return !this.statementName.isEmpty() && getCache() == null;
    }

    @Nullable
    @Override
    public PostgreStmt getCache() {
        return null;
    }

    @Override
    public int getFetchSize() {
        return this.fetchSize;
    }

    @Override
    public String getReplacedSql() {
        return this.cacheStmt.postgreSql();
    }

    @Override
    public String getStatementName() {
        return this.statementName;
    }

    @Override
    public Publisher<ByteBuf> prepare() {
        if (this.oneRoundTrip) {
            // no bug,never here
            throw new IllegalStateException("Current is  one shot");
        }
        final List<ByteBuf> messageList = new ArrayList<>(2);
        messageList.add(createParseMessage());    // Parse message
        appendDescribeMessage(messageList, true); // Describe message for statement
        appendSyncMessage(messageList);           // Sync message
        return Flux.fromIterable(messageList);
    }

    @Override
    public Publisher<ByteBuf> executeOneRoundTrip() {
        final List<ParamValue> bindGroup = obtainBindGroupForOneShot();
        if (bindGroup.size() > 0 || !this.oneRoundTrip) {
            throw new IllegalStateException("Not one shot");
        }

        beforeExecute();

        final List<ByteBuf> messageList = new ArrayList<>(3);

        messageList.add(createParseMessage());    // Parse message
        messageList.add(createBindMessage(0, 0)); // Bind message
        appendDescribeMessage(messageList, false);// Describe message for portal
        appendExecuteMessage(messageList);        // Execute message

        appendSyncMessage(messageList);           // Sync message
        return Flux.fromIterable(messageList);
    }


    @Override
    public Publisher<ByteBuf> bindAndExecute() {
        if (this.paramTypeList != null) {
            throw new IllegalStateException("duplication execute.");
        }

        beforeExecute();

        this.paramTypeList = this.stmtTask.getParamTypes();
        return Flux.create(sink -> {
            if (this.adjutant.inEventLoop()) {
                continueBindExecuteInEventLoop(sink, 0);
            } else {
                this.adjutant.execute(() -> continueBindExecuteInEventLoop(sink, 0));
            }
        });
    }

    @Override
    public Publisher<ByteBuf> fetch() {
        if (!supportFetch()) {
            throw new IllegalStateException("Not support fetch.");
        }
        List<ByteBuf> messageList = new ArrayList<>(1);
        appendExecuteMessage(messageList);
        return Flux.fromIterable(messageList);
    }

    @Override
    public Publisher<ByteBuf> closeStatement() {
        if (!isNeedClose()) {
            throw new IllegalStateException("Don't need close.");
        }
        final byte[] nameBytes = this.statementName.getBytes(this.adjutant.clientCharset());
        final int length = 7 + nameBytes.length;
        final ByteBuf message = this.adjutant.allocator().buffer(length + 1);

        message.writeByte(Messages.C);
        message.writeInt(length);
        message.writeByte('S');
        message.writeBytes(nameBytes);
        message.writeByte(Messages.STRING_TERMINATOR);

        return Mono.just(message);
    }

    /**
     * @see #executeOneRoundTrip()
     * @see #bindAndExecute()
     */
    private void beforeExecute() {
        if (PgStrings.hasText(this.portalName)) {
            throw new IllegalStateException("duplication execute");
        }
        if (this.stmtTask.getRowMeta() != null) {
            final int fetchSize = getFetchSizeFromStmt(this.stmt);
            if (fetchSize > 0) {
                this.fetchSize = fetchSize;
                this.portalName = this.adjutant.createPortalName();
            }
        }
    }


    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Parse (F)</a>
     */
    private ByteBuf createParseMessage() {
        final Charset charset = this.adjutant.clientCharset();

        final ByteBuf message = this.adjutant.allocator().buffer(1024, Integer.MAX_VALUE);
        //  write Parse message
        message.writeByte(Messages.P);
        message.writeZero(Messages.LENGTH_SIZE); // placeholder of length
        final String statementName = this.statementName;
        if (!statementName.isEmpty()) {
            message.writeBytes(statementName.getBytes(charset)); //definite statement name for caching statement
        }
        message.writeByte(Messages.STRING_TERMINATOR);

        message.writeBytes(this.replacedSql.getBytes(charset));
        message.writeByte(Messages.STRING_TERMINATOR);

        message.writeShort(0); //jdbd-postgre don't support parameter oid in Parse message,@see /document/note/Q&A.md

        Messages.writeLength(message);
        return message;
    }


    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Execute</a>
     */
    private void appendExecuteMessage(List<ByteBuf> messageList) {
        final boolean supportFetch = supportFetch();
        final byte[] portalNameBytes;
        if (supportFetch) {
            final String portalName = Objects.requireNonNull(this.portalName, "this.portalName");
            portalNameBytes = portalName.getBytes(this.adjutant.clientCharset());
        } else {
            portalNameBytes = new byte[0];
        }
        final int length = 9 + portalNameBytes.length;
        final ByteBuf message = getByteBufByNeedCapacity(messageList, length + 1);

        message.writeByte(Messages.E);
        message.writeInt(length);
        if (portalNameBytes.length > 0) {
            message.writeBytes(portalNameBytes);
        }
        message.writeByte(Messages.STRING_TERMINATOR);
        if (supportFetch) {
            message.writeInt(this.fetchSize);
        } else {
            message.writeInt(0);
        }

    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Describe</a>
     */
    private void appendDescribeMessage(final List<ByteBuf> messageList, final boolean describeStatement) {
        final String name = describeStatement ? this.statementName : this.portalName;
        final byte[] nameBytes;
        if (PgStrings.hasText(name)) {
            nameBytes = name.getBytes(this.adjutant.clientCharset());
        } else {
            nameBytes = new byte[0];
        }
        final int length = 6 + nameBytes.length, needCapacity = 1 + length;

        final ByteBuf message = getByteBufByNeedCapacity(messageList, needCapacity);

        message.writeByte(Messages.D);
        message.writeInt(length);
        message.writeByte(describeStatement ? 'S' : 'P');
        if (nameBytes.length > 0) {
            message.writeBytes(nameBytes);
        }
        message.writeByte(Messages.STRING_TERMINATOR);

    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Sync</a>
     */
    private void appendSyncMessage(final List<ByteBuf> messageList) {
        writeSyncMessage(getByteBufByNeedCapacity(messageList, 5));
    }

    /**
     * @see #appendSyncMessage(List)
     */
    private void writeSyncMessage(ByteBuf message) {
        message.writeByte(Messages.S);
        message.writeInt(Messages.LENGTH_SIZE); // length
    }

    /**
     * @see #appendDescribeMessage(List, boolean)
     * @see #appendExecuteMessage(List)
     * @see #appendSyncMessage(List)
     */
    private ByteBuf getByteBufByNeedCapacity(final List<ByteBuf> messageList, final int needCapacity) {
        final int messageSize = messageList.size();
        final ByteBuf message;
        if (messageSize > 0) {// netty generally return cache ByteBuf with capacity 1024,so we can append.
            final ByteBuf lastMessage = messageList.get(messageSize - 1);
            if (lastMessage.isReadOnly() || lastMessage.writableBytes() < needCapacity) {
                message = this.adjutant.allocator().buffer(needCapacity);
                messageList.add(message);
            } else {
                message = lastMessage;
            }
        } else {
            message = this.adjutant.allocator().buffer(needCapacity);
            messageList.add(message);
        }
        return message;
    }


    /**
     * @see #executeOneRoundTrip()
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Bind</a>
     */
    private ByteBuf createBindMessage(final int batchIndex, final int bindCount)
            throws JdbdSQLException {

        final Charset clientCharset = this.adjutant.clientCharset();
        final ByteBuf message = this.adjutant.allocator().buffer(1024);
        message.writeByte(Messages.B);
        message.writeZero(Messages.LENGTH_SIZE);//placeholder of length
        // The name of the destination portal (an empty string selects the unnamed portal).
        if (supportFetch()) {
            final String portalName = Objects.requireNonNull(this.portalName, "this.portalName");
            message.writeBytes(portalName.getBytes(clientCharset));
        }
        message.writeByte(Messages.STRING_TERMINATOR);
        // The name of the source prepared statement (an empty string selects the unnamed prepared statement).
        final String statementName = this.statementName;
        if (!statementName.equals("")) {
            message.writeBytes(statementName.getBytes(clientCharset));
        }
        message.writeByte(Messages.STRING_TERMINATOR);

        final List<PgType> paramTypeList = this.oneRoundTrip ? Collections.emptyList() : this.stmtTask.getParamTypes();
        final int paramCount = paramTypeList.size();
        if (bindCount != paramCount) {
            throw PgExceptions.parameterCountMatch(batchIndex, paramCount, bindCount);
        }
        message.writeShort(paramCount); // The number of parameter format codes
        for (PgType type : paramTypeList) {
            message.writeShort(PgBinds.decideFormatCode(type));
        }
        message.writeShort(paramCount); // The number of parameter values
        if (this.oneRoundTrip) { // one shot.
            message.writeShort(paramCount); // result format count
            Messages.writeLength(message);
        }
        return message;
    }

    private String replacePlaceholder(PgStatement statement) {

        final List<String> staticSqlList = statement.sqlPartList();
        String sql;
        if (staticSqlList.size() == 1) {
            sql = this.stmt.getSql();
        } else {
            final StringBuilder builder = new StringBuilder(statement.originalSql().length() + staticSqlList.size());
            final int paramCount = staticSqlList.size() - 1;
            for (int i = 0; i < paramCount; i++) {
                builder.append(staticSqlList.get(i))
                        .append('$')
                        .append(i + 1);
            }
            builder.append(staticSqlList.get(paramCount));
            sql = builder.toString();
        }
        return sql;
    }

    /**
     * @throws IllegalStateException stmt when not one shot.
     */
    private List<ParamValue> obtainBindGroupForOneShot() throws IllegalStateException {
        final ParamSingleStmt stmt = this.stmt;
        final List<ParamValue> bindGroup;
        if (stmt instanceof ParamStmt) {
            bindGroup = ((ParamStmt) stmt).getBindGroup();
        } else if (stmt instanceof ParamBatchStmt) {
            final List<List<ParamValue>> groupList = ((ParamBatchStmt) stmt).getGroupList();
            if (groupList.size() != 1) {
                throw new IllegalStateException("stmt not one shot.");
            }
            bindGroup = groupList.get(0);
        } else {
            throw new IllegalStateException("stmt not one shot.");
        }
        return bindGroup;
    }

    /**
     * @see #bindAndExecute()
     * @see ParameterSubscriber#onCompleteInEventLoop()
     */
    private void continueBindExecuteInEventLoop(FluxSink<ByteBuf> channelSink, int batchIndex) {
        ByteBuf message = null;
        try {
            List nextBindGroup = getBindGroup(batchIndex);
            while (nextBindGroup != null) {
                message = createBindMessage(batchIndex, nextBindGroup.size());
                if (continueWriteBindParam(message, batchIndex, 0, nextBindGroup, channelSink)) {
                    nextBindGroup = handBindComplete(message, batchIndex, channelSink);
                } else {
                    // exists Publisher type parameter.
                    break;
                }
                message = null; // avoid createBindMessage throw error.
                batchIndex++;
            }
        } catch (Throwable e) {
            if (message != null) {
                handleBindError(message, e, batchIndex, channelSink);
            }

        }
    }


    /**
     * @see #continueWriteBindParam(ByteBuf, int, int, List, FluxSink)
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Bind message</a>
     */
    private void writeNonNullBindValue(final ByteBuf message, final int batchIndex, final PgType pgType,
                                       final ParamValue paramValue) {

        final Charset clientCharset = this.adjutant.clientCharset();

        switch (pgType) {
            case BOOLEAN: {// binary format
                if (PgBinds.bindToBoolean(batchIndex, pgType, paramValue)) {
                    message.writeByte(1);
                } else {
                    message.writeByte(0);
                }
            }
            break;
            case SMALLINT: {// binary format
                message.writeShort(PgBinds.bindToShort(batchIndex, pgType, paramValue));
            }
            break;
            case INTEGER: {// binary format
                message.writeInt(PgBinds.bindToInt(batchIndex, pgType, paramValue));
            }
            break;
            case OID:
            case BIGINT: {// binary format
                message.writeLong(PgBinds.bindToLong(batchIndex, pgType, paramValue));
            }
            break;
            case REAL: {// binary format
                final float value = PgBinds.bindToFloat(batchIndex, pgType, paramValue);
                message.writeInt(Float.floatToIntBits(value));
            }
            break;
            case FLOAT8: {// binary format
                final double value = PgBinds.bindToDouble(batchIndex, pgType, paramValue);
                message.writeLong(Double.doubleToLongBits(value));
            }
            break;
            case DECIMAL: {// text format
                final Object bindValue = paramValue.getNonNull();
                final String value;
                if (bindValue instanceof String && PgConstant.NaN.equalsIgnoreCase((String) bindValue)) {
                    value = (String) bindValue;
                } else {
                    value = PgBinds.bindToDecimal(batchIndex, pgType, paramValue).toPlainString();
                }
                message.writeBytes(value.getBytes(clientCharset));
            }
            break;
            case NUMRANGE:
            case DATERANGE:
            case TSRANGE:
            case INT4RANGE:
            case INT8RANGE:
            case TSTZRANGE: // all range type is text format
            case MACADDR8:
            case MACADDR:
            case INET:
            case CIDR:
            case MONEY:
            case UUID: {
                final String value;
                value = PgBinds.bindToString(batchIndex, pgType, paramValue);
                message.writeBytes(value.getBytes(clientCharset));
            }
            break;
            case VARCHAR:
            case CHAR: {
                final Object nonNull = paramValue.getNonNull();
                if (nonNull instanceof byte[]) {
                    message.writeBytes((byte[]) nonNull);
                } else {
                    final String value;
                    value = PgBinds.bindToString(batchIndex, pgType, paramValue);
                    message.writeBytes(value.getBytes(clientCharset));
                }
            }
            break;
            case BIT: {// text format
                final String value;
                value = PgBinds.bindNonNullToBit(batchIndex, pgType, paramValue);
                message.writeBytes(value.getBytes(clientCharset));
            }
            break;
            case VARBIT: {// text format
                final String value;
                value = PgBinds.bindNonNullToVarBit(batchIndex, pgType, paramValue);
                message.writeBytes(value.getBytes(clientCharset));
            }
            break;
            case BYTEA: {// binary format
                bindNonNullToBytea(message, batchIndex, pgType, paramValue);
            }
            break;
            case TIME: {// text format
                final String value;
                value = PgBinds.bindToLocalTime(batchIndex, pgType, paramValue)
                        .format(PgTimes.ISO_LOCAL_TIME_FORMATTER);
                message.writeBytes(value.getBytes(clientCharset));
            }
            break;
            case TIMETZ: {// text format
                final String value;
                value = PgBinds.bindToOffsetTime(batchIndex, pgType, paramValue)
                        .format(PgTimes.ISO_OFFSET_TIME_FORMATTER);
                message.writeBytes(value.getBytes(clientCharset));
            }
            break;
            case DATE: {// text format
                writeNonNullToDate(message, batchIndex, pgType, paramValue);
            }
            break;
            case TIMESTAMP: {// text format
                writeNonNullToTimestamp(message, batchIndex, pgType, paramValue);
            }
            break;
            case TIMESTAMPTZ: {// text format
                writeNonNullToTimestampTz(message, batchIndex, pgType, paramValue);
            }
            break;
            case INTERVAL: {// text format
                final String value;
                value = PgBinds.bindToInterval(batchIndex, pgType, paramValue);
                message.writeBytes(value.getBytes(clientCharset));
            }
            break;
            // below bind array with non-array value.
            case BOOLEAN_ARRAY:
            case SMALLINT_ARRAY:
            case INTEGER_ARRAY:
            case OID_ARRAY:
            case BIGINT_ARRAY:
            case DECIMAL_ARRAY:
            case REAL_ARRAY:
            case FLOAT8_ARRAY:
            case BIT_ARRAY:
            case VARBIT_ARRAY:
            case TIME_ARRAY:
            case DATE_ARRAY:
            case TIMESTAMP_ARRAY:
            case TIMETZ_ARRAY:
            case TIMESTAMPTZ_ARRAY:
            case UUID_ARRAY:
            case INTERVAL_ARRAY:
            case MONEY_ARRAY:
            case NUMRANGE_ARRAY:
            case DATERANGE_ARRAY:
            case INT4RANGE_ARRAY:
            case INT8RANGE_ARRAY:
            case TSTZRANGE_ARRAY:
            case TSRANGE_ARRAY:

            case INET_ARRAY:
            case CIDR_ARRAY:
            case MACADDR8_ARRAY:
            case MACADDR_ARRAY:

            case POINT_ARRAY:
            case LINE_ARRAY:
            case LSEG_ARRAY:
            case BOX_ARRAY:
            case PATH_ARRAY:
            case CIRCLE_ARRAY:
            case POLYGON_ARRAY:
            case TSVECTOR_ARRAY:
            case TSQUERY_ARRAY:
            case TEXT_ARRAY:
            case XML_ARRAY:
            case CHAR_ARRAY:
            case VARCHAR_ARRAY:
            case BYTEA_ARRAY:
            case JSON_ARRAY:
            case JSONB_ARRAY:
                // all geometry type is text format
            case POINT:
            case CIRCLE:
            case LINE:
            case PATH:
            case POLYGON:
            case LSEG:
            case BOX:
            case XML:
            case TEXT:
            case JSON:
            case JSONB:
            case TSQUERY:
            case TSVECTOR:
            case UNSPECIFIED: {// text format
                this.bindNonNullToLongString(message, batchIndex, pgType, paramValue);
            }
            break;
            case REF_CURSOR_ARRAY:
            case REF_CURSOR:
            default:
                throw PgExceptions.createUnexpectedEnumException(pgType);
        }
    }

    /**
     * @see #continueWriteBindParam(ByteBuf, int, int, List, FluxSink)
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Bind message</a>
     */
    private void writeNonNullArray(final int batchIndex, final PgType pgType, ParamValue paramValue, ByteBuf message)
            throws SQLException {

        final Charset clientCharset = this.adjutant.clientCharset();

        final String v;
        switch (pgType) {
            case BOOLEAN_ARRAY: {
                v = PgBinds.bindToBooleanArray(batchIndex, pgType, paramValue);
            }
            break;
            case SMALLINT_ARRAY: {
                v = PgBinds.bindNonNullSmallIntArray(batchIndex, pgType, paramValue);
            }
            break;
            case INTEGER_ARRAY: {
                v = PgBinds.bindNonNullIntegerArray(batchIndex, pgType, paramValue);
            }
            break;
            case OID_ARRAY:
            case BIGINT_ARRAY: {
                v = PgBinds.bindNonNullBigIntArray(batchIndex, pgType, paramValue);
            }
            break;
            case DECIMAL_ARRAY: {
                v = PgBinds.bindNonNullDecimalArray(batchIndex, pgType, paramValue);
            }
            break;
            case REAL_ARRAY: {
                v = PgBinds.bindNonNullFloatArray(batchIndex, pgType, paramValue);
            }
            break;
            case FLOAT8_ARRAY: {
                v = PgBinds.bindNonNullDoubleArray(batchIndex, pgType, paramValue);
            }
            break;
            case BIT_ARRAY: {
                v = PgBinds.bindNonNullBitArray(batchIndex, pgType, paramValue);
            }
            break;
            case VARBIT_ARRAY: {
                v = PgBinds.bindNonNullVarBitArray(batchIndex, pgType, paramValue);
            }
            break;
            case TIME_ARRAY: {
                v = PgBinds.bindNonNullTimeArray(batchIndex, pgType, paramValue);
            }
            break;
            case DATE_ARRAY: {
                v = PgBinds.bindNonNullDateArray(batchIndex, pgType, paramValue);
            }
            break;
            case TIMESTAMP_ARRAY: {
                v = PgBinds.bindNonNullTimestampArray(batchIndex, pgType, paramValue);
            }
            break;
            case TIMETZ_ARRAY: {
                v = PgBinds.bindNonNullTimeTzArray(batchIndex, pgType, paramValue);
            }
            break;
            case TIMESTAMPTZ_ARRAY: {
                v = PgBinds.bindNonNullTimestampTzArray(batchIndex, pgType, paramValue);
            }
            break;
            case UUID_ARRAY: {
                v = PgBinds.bindNonNullUuidArray(batchIndex, pgType, paramValue);
            }
            break;
            case INTERVAL_ARRAY: {
                v = PgBinds.bindNonNullIntervalArray(batchIndex, pgType, paramValue);
            }
            break;
            case MONEY_ARRAY: {
                v = PgBinds.bindNonNullMoneyArray(batchIndex, pgType, paramValue);
            }
            break;
            case BYTEA_ARRAY: {
                v = PgBinds.bindNonNullByteaArray(batchIndex, pgType, paramValue, clientCharset);
            }
            break;
            case NUMRANGE_ARRAY:
            case DATERANGE_ARRAY:
            case INT4RANGE_ARRAY:
            case INT8RANGE_ARRAY:
            case TSTZRANGE_ARRAY:
            case TSRANGE_ARRAY:

            case INET_ARRAY:
            case CIDR_ARRAY:
            case MACADDR8_ARRAY:
            case MACADDR_ARRAY:

            case POINT_ARRAY:
            case LINE_ARRAY:
            case LSEG_ARRAY:
            case BOX_ARRAY:
            case PATH_ARRAY:
            case CIRCLE_ARRAY:
            case POLYGON_ARRAY: {
                v = PgBinds.bindNonNullSafeTextArray(batchIndex, pgType, paramValue);
            }
            break;
            case TSVECTOR_ARRAY:
            case TSQUERY_ARRAY:
            case TEXT_ARRAY:
            case XML_ARRAY:
            case JSON_ARRAY:
            case JSONB_ARRAY:
            case CHAR_ARRAY:
            case VARCHAR_ARRAY:
            case UNSPECIFIED: {
                v = PgBinds.bindNonNullEscapesTextArray(batchIndex, pgType, paramValue);
            }
            break;
            case REF_CURSOR_ARRAY:
            default: {
                throw PgExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
            }
        }

        message.writeBytes(v.getBytes(clientCharset));

    }


    /**
     * @see #writeNonNullBindValue(ByteBuf, int, PgType, ParamValue)
     */
    private void bindNonNullToBytea(ByteBuf message, final int batchIndex, PgType pgType, ParamValue paramValue)
            throws LocalFileException, SQLException {
        if (PgBinds.decideFormatCode(pgType) != 1) {
            throw new IllegalStateException("format code error.");
        }
        final Object nonNull = paramValue.getNonNull();
        if (nonNull instanceof Path) {
            writePathWithBinary(message, batchIndex, pgType, paramValue);
        } else {
            final byte[] value;
            if (nonNull instanceof byte[]) {
                value = (byte[]) nonNull;
            } else if (nonNull instanceof String) {
                value = ((String) nonNull).getBytes(this.adjutant.clientCharset());
            } else {
                throw PgExceptions.createNotSupportBindTypeError(batchIndex, BindValue.wrap(pgType, paramValue));
            }
            if (message.maxWritableBytes() < value.length) {
                throw PgExceptions.beyondMessageLength(batchIndex, paramValue);
            }
            message.writeBytes(value);
        }
    }

    /**
     * @see #writeNonNullBindValue(ByteBuf, int, PgType, ParamValue)
     */
    private void writeNonNullToDate(ByteBuf message, final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        final Object bindValue = paramValue.getNonNull();
        final String value;
        if (bindValue instanceof String) {
            final String v = ((String) bindValue).toLowerCase();
            switch (v) {
                case PgConstant.INFINITY:
                case PgConstant.NEG_INFINITY:
                    value = v;
                    break;
                default:
                    value = PgBinds.bindToLocalDate(batchIndex, pgType, paramValue)
                            .format(PgTimes.PG_ISO_LOCAL_DATE_FORMATTER);
            }
        } else {
            value = PgBinds.bindToLocalDate(batchIndex, pgType, paramValue)
                    .format(PgTimes.PG_ISO_LOCAL_DATE_FORMATTER);
        }
        message.writeBytes(value.getBytes(this.adjutant.clientCharset()));
    }

    /**
     * @see #writeNonNullBindValue(ByteBuf, int, PgType, ParamValue)
     */
    private void writeNonNullToTimestamp(ByteBuf message, final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        final Object bindValue = paramValue.getNonNull();
        final String value;
        if (bindValue instanceof String) {
            final String v = ((String) bindValue).toLowerCase();
            switch (v) {
                case PgConstant.INFINITY:
                case PgConstant.NEG_INFINITY:
                    value = v;
                    break;
                default:
                    value = PgBinds.bindToLocalDateTime(batchIndex, pgType, paramValue)
                            .format(PgTimes.PG_ISO_LOCAL_DATETIME_FORMATTER);
            }
        } else {
            value = PgBinds.bindToLocalDateTime(batchIndex, pgType, paramValue)
                    .format(PgTimes.PG_ISO_LOCAL_DATETIME_FORMATTER);
        }
        message.writeBytes(value.getBytes(this.adjutant.clientCharset()));
    }

    /**
     * @see #writeNonNullBindValue(ByteBuf, int, PgType, ParamValue)
     */
    private void writeNonNullToTimestampTz(ByteBuf message, final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        final Object bindValue = paramValue.getNonNull();
        final String value;
        if (bindValue instanceof String) {
            final String v = ((String) bindValue).toLowerCase();
            switch (v) {
                case PgConstant.INFINITY:
                case PgConstant.NEG_INFINITY:
                    value = v;
                    break;
                default:
                    value = PgBinds.bindToOffsetDateTime(batchIndex, pgType, paramValue)
                            .format(PgTimes.PG_ISO_OFFSET_DATETIME_FORMATTER);
            }
        } else {
            value = PgBinds.bindToOffsetDateTime(batchIndex, pgType, paramValue)
                    .format(PgTimes.PG_ISO_OFFSET_DATETIME_FORMATTER);
        }
        message.writeBytes(value.getBytes(this.adjutant.clientCharset()));
    }


    /**
     * @see #writeNonNullBindValue(ByteBuf, int, PgType, ParamValue)
     */
    private void bindNonNullToLongString(ByteBuf message, final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException, LocalFileException {
        final Object bindValue = paramValue.getNonNull();
        if (bindValue instanceof Path) {
            writePathWithString(message, batchIndex, pgType, paramValue);
        } else if (bindValue instanceof byte[]) {
            message.writeBytes((byte[]) bindValue);
        } else {
            final String text = PgBinds.bindToString(batchIndex, pgType, paramValue);
            message.writeBytes(text.getBytes(this.adjutant.clientCharset()));
        }
    }


    private void writePathWithString(ByteBuf message, final int batchIndex, PgType pgType, ParamValue paramValue)
            throws LocalFileException, SQLException {
        try (FileChannel channel = FileChannel.open((Path) paramValue.getNonNull(), StandardOpenOption.READ)) {
            final long size = channel.size();
            if (size >= message.maxWritableBytes()) {
                throw PgExceptions.beyondMessageLength(batchIndex, paramValue);
            }
            final Charset clientCharset = this.adjutant.clientCharset();
            final boolean isUtf8 = clientCharset.equals(StandardCharsets.UTF_8);
            final byte[] bufferArray = new byte[(int) Math.min(2048, size)];
            final ByteBuffer buffer = ByteBuffer.wrap(bufferArray);

            while (channel.read(buffer) > 0) {
                buffer.flip();
                if (isUtf8) {
                    message.writeBytes(bufferArray, 0, buffer.limit());
                } else {
                    final byte[] writeBytes = new String(bufferArray, 0, buffer.limit(), StandardCharsets.UTF_8)
                            .getBytes(clientCharset);
                    message.writeBytes(writeBytes);
                }
                buffer.clear();
            }
        } catch (Throwable e) {
            if (PgExceptions.isByteBufOutflow(e)) {
                throw PgExceptions.beyondMessageLength(batchIndex, paramValue);
            } else {
                throw PgExceptions.localFileWriteError(batchIndex, pgType, paramValue, e);
            }

        }
    }


    @Nullable
    private List getBindGroup(final int batchIndex) throws IllegalArgumentException {
        ParamSingleStmt stmt = this.stmt;
        if (stmt instanceof PrepareStmt) {
            stmt = ((PrepareStmt) stmt).getStmt();
        }
        final List bindGroup;
        if (stmt instanceof ParamStmt) {
            switch (batchIndex) {
                case 0:
                    bindGroup = ((ParamStmt) stmt).getBindGroup();
                    break;
                case 1:
                    bindGroup = null;
                    break;
                default:
                    throw new IllegalArgumentException(String.format("batchIndex[%s] not in [0,1]", batchIndex));
            }

        } else {
            final ParamBatchStmt batchStmt = (ParamBatchStmt) stmt;
            final int groupCount = batchStmt.getGroupList().size();
            if (batchIndex < 0 || batchIndex > groupCount) {
                String msg = String.format("batchIndex[%s] not in [0,%s)", batchStmt, groupCount);
                throw new IllegalArgumentException(msg);
            }
            if (batchIndex == groupCount) {
                bindGroup = null;
            } else {
                bindGroup = batchStmt.getGroupList().get(batchIndex);
            }
        }
        return bindGroup;
    }


    /**
     * @return true : no {@link Publisher} ,all parameter write complete; false : async bind {@link Publisher}
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Bind</a>
     * @see #continueBindExecuteInEventLoop(FluxSink, int)
     */
    private boolean continueWriteBindParam(ByteBuf message, final int batchIndex, int paramIndex
            , List bindGroup, FluxSink<ByteBuf> channelSink) throws SQLException {

        final List<PgType> paramTypeList = Objects.requireNonNull(this.paramTypeList, "this.paramTypeList");
        final int paramCount = paramTypeList.size();
        if (paramIndex < 0 || (paramCount > 0 && paramIndex >= paramCount)) {
            throw new IllegalArgumentException(String.format("paramIndex[%s] error.", paramIndex));
        }
        if (bindGroup.size() != paramCount) {
            throw PgExceptions.createBindCountNotMatchError(batchIndex, paramCount, bindGroup.size());
        }

        for (int valueLengthIndex, valueEndIndex; paramIndex < paramCount; paramIndex++) {
            final ParamValue paramValue = bindGroup.get(paramIndex);
            final PgType pgType = paramTypeList.get(paramIndex);

            if (paramValue.getIndex() != paramIndex) {
                final BindValue bindValue = BindValue.wrap(pgType, paramValue);
                throw PgExceptions.createBindIndexNotMatchError(batchIndex, paramIndex, bindValue);
            }
            final Object value = paramValue.get();
            if (value == null) {
                message.writeInt(-1); //  -1 indicates a NULL parameter value
                continue;
            }

            if (value instanceof Publisher) {
                if (!pgType.supportPublisher()) {
                    throw PgExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
                }
                final ParameterSubscriber subscriber;
                subscriber = new ParameterSubscriber(message, batchIndex, paramIndex, pgType, channelSink);
                final Publisher<?> publisher = (Publisher<?>) value;
                publisher.subscribe(subscriber);
                break;  // break loop , async bind
            } else {
                valueLengthIndex = message.writerIndex();
                message.writeZero(4); // placeholder of parameter value length.
                if (!(value instanceof byte[]) && value.getClass().isArray()) {
                    writeNonNullArray(batchIndex, pgType, paramValue, message);// write array parameter
                } else {
                    writeNonNullBindValue(message, batchIndex, pgType, paramValue); // write non-array parameter
                }
                valueEndIndex = message.writerIndex();

                message.writerIndex(valueLengthIndex);
                message.writeInt(valueEndIndex - valueLengthIndex - 4);
                message.writerIndex(valueEndIndex);
            }
        }

        final boolean bindFinish = paramIndex == paramCount;
        if (bindFinish) {
            if (message.maxWritableBytes() < (paramCount << 1) + 2) {
                throw PgExceptions.tooLargeObject();
            }
            final ResultRowMeta rowMeta = this.stmtTask.getRowMeta();
            if (rowMeta == null) {
                message.writeShort(0);
            } else {
                final int columnCount = rowMeta.getColumnCount();
                message.writeShort(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    // write result format
                    message.writeShort(PgBinds.decideFormatCode((PgType) rowMeta.getSQLType(i)));
                }
            }
        }
        return bindFinish;

    }

    /**
     * @see #continueBindExecuteInEventLoop(FluxSink, int)
     * @see ParameterSubscriber#onCompleteInEventLoop()
     */
    @Nullable
    private List handBindComplete(final ByteBuf bindMessage, final int batchIndex
            , FluxSink<ByteBuf> channelSink) {


        final List nextBindGroup = getBindGroup(batchIndex + 1);

        Messages.writeLength(bindMessage); // write bind message length .

        final List<ByteBuf> messageList = new ArrayList<>(2);
        messageList.add(bindMessage);               // Bind message
        if (this.stmtTask.getRowMeta() != null) {
            appendDescribeMessage(messageList, false);  // Describe message for portal
        }
        appendExecuteMessage(messageList);          // Execute message

        if (nextBindGroup == null) {
            appendSyncMessage(messageList);         // Sync message
        }

        for (ByteBuf message : messageList) {
            channelSink.next(message);
        }
        if (nextBindGroup == null) {
            channelSink.complete();
        }
        return nextBindGroup;
    }

    private void handleBindError(final ByteBuf message, Throwable error, final int batchIndex
            , FluxSink<ByteBuf> channelSInk) {

        if (error instanceof IndexOutOfBoundsException
                && (Integer.MAX_VALUE - message.readableBytes()) < 1024) { // here only simple type ,eg: int,boolean
            this.stmtTask.addErrorToTask(PgExceptions.tooLargeObject());
        } else {
            this.stmtTask.addErrorToTask(error);
        }
        if (message.refCnt() > 0) {
            message.release();
        }

        if (batchIndex > 0) {
            final ByteBuf syncMessage = this.adjutant.allocator().buffer(5);
            writeSyncMessage(syncMessage);
            channelSInk.next(syncMessage);
        }
        channelSInk.complete(); // don't emit error to netty channel
        if (batchIndex == 0) {
            // Task need end task.
            this.stmtTask.handleNoExecuteMessage();
        }
    }



    /*################################## blow private static method ##################################*/

    /**
     * @see #bindNonNullToBytea(ByteBuf, int, PgType, ParamValue)
     */
    private static void writePathWithBinary(ByteBuf message, final int batchIndex, PgType pgType, ParamValue paramValue)
            throws LocalFileException, SQLException {
        try (FileChannel channel = FileChannel.open((Path) paramValue.getNonNull(), StandardOpenOption.READ)) {
            final long size = channel.size();
            if (size >= message.maxWritableBytes()) {
                throw PgExceptions.beyondMessageLength(batchIndex, paramValue);
            }
            final byte[] bufferArray = new byte[(int) Math.min(2048, size)];
            final ByteBuffer buffer = ByteBuffer.wrap(bufferArray);

            while (channel.read(buffer) > 0) {
                buffer.flip();
                message.writeBytes(bufferArray, 0, buffer.limit());
                buffer.clear();
            }
        } catch (Throwable e) {
            if (PgExceptions.isByteBufOutflow(e)) {
                throw PgExceptions.beyondMessageLength(batchIndex, paramValue);
            } else {
                throw PgExceptions.localFileWriteError(batchIndex, pgType, paramValue, e);
            }
        }
    }

    private static boolean isOneShotStmt(final ParamSingleStmt stmt) {
        final boolean oneShot;
        if (stmt instanceof ParamStmt) {
            oneShot = stmt.getFetchSize() == 0;
        } else if (stmt instanceof ParamBatchStmt) {
            oneShot = ((ParamBatchStmt) stmt).getGroupList().size() > 1 || stmt.getFetchSize() == 0;
        } else {
            oneShot = false;
        }
        return oneShot;
    }

    private static boolean isOnlyOneBindGroup(ParamSingleStmt stmt) {
        final boolean onlyOne;
        if (stmt instanceof PrepareStmt) {
            stmt = ((PrepareStmt) stmt).getStmt();
        }
        if (stmt instanceof ParamStmt) {
            onlyOne = true;
        } else {
            final ParamBatchStmt batchStmt = (ParamBatchStmt) stmt;
            onlyOne = batchStmt.getGroupList().size() == 1;
        }
        return onlyOne;
    }

    private static int getFirstBatchBindCount(final ParamSingleStmt stmt) {
        final int bindCount;
        if (stmt instanceof ParamStmt) {
            bindCount = ((ParamStmt) stmt).getBindGroup().size();
        } else {
            final ParamBatchStmt batchStmt = (ParamBatchStmt) stmt;
            if (batchStmt.getGroupList().isEmpty()) {
                bindCount = 0;
            } else {
                bindCount = batchStmt.getGroupList().get(0).size();
            }
        }
        return bindCount;
    }

    private static int getFetchSizeFromStmt(ParamSingleStmt stmt) {
        if (stmt instanceof PrepareStmt) {
            stmt = ((PrepareStmt) stmt).getStmt();
        }
        final int fetchSize;
        if (stmt instanceof ParamStmt) {
            fetchSize = stmt.getFetchSize();
        } else {
            final ParamBatchStmt batchStmt = (ParamBatchStmt) stmt;
            if (batchStmt.getGroupList().size() == 1) {
                fetchSize = batchStmt.getFetchSize();
            } else {
                fetchSize = 0;
            }
        }
        return fetchSize;
    }

    private final class ParameterSubscriber implements Subscriber<Object> {

        private final int valueLengthIndex;

        private final int paramIndex;

        private final FluxSink<ByteBuf> channelSInk;

        private final TaskAdjutant adjutant;

        private final boolean binaryData;

        private final ByteBuf message;

        private final int batchIndex;

        private final Charset clientCharset;

        private Subscription subscription;

        private Throwable error;

        private boolean end;

        private ParameterSubscriber(ByteBuf message, final int batchIndex, int paramIndex
                , PgType pgType, FluxSink<ByteBuf> channelSInk) {
            this.valueLengthIndex = message.writerIndex();
            message.writeZero(Messages.LENGTH_SIZE); // placeholder parameter value length

            this.message = message;
            this.batchIndex = batchIndex;
            this.paramIndex = paramIndex;
            this.channelSInk = channelSInk;

            this.adjutant = PgExtendedCommandWriter.this.adjutant;
            this.binaryData = pgType.supportBinaryPublisher();
            this.clientCharset = adjutant.clientCharset();
        }

        @Override
        public void onSubscribe(Subscription s) {
            this.subscription = s;
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(final Object obj) {
            if (this.error != null) {
                return;
            }
            if (this.adjutant.inEventLoop()) {
                onNextInEventLoop(obj);
            } else {
                this.adjutant.execute(() -> onNextInEventLoop(obj));
            }
        }

        @Override
        public void onError(final Throwable t) {
            if (this.adjutant.inEventLoop()) {
                onErrorInEventLoop(t);
            } else {
                this.adjutant.execute(() -> onErrorInEventLoop(t));
            }
        }

        @Override
        public void onComplete() {
            if (this.adjutant.inEventLoop()) {
                onCompleteInEventLoop();
            } else {
                this.adjutant.execute(this::onCompleteInEventLoop);
            }
        }

        private void onCompleteInEventLoop() {
            if (this.end) {
                return;
            }
            this.end = true;
            List nextBindGroup = null;
            final ByteBuf message = this.message;
            try {
                List bindGroup = getBindGroup(this.batchIndex);
                if (bindGroup == null) {
                    String msg = String.format("batchIndex[%s] not find any bind.", this.batchIndex);
                    throw new IllegalStateException(msg);
                }

                // write value length
                final int valueEndIndex = message.writerIndex();
                message.writerIndex(this.valueLengthIndex);
                message.writeInt(valueEndIndex - this.valueLengthIndex - Messages.LENGTH_SIZE);
                message.writerIndex(valueEndIndex);

                final int nextIndex = this.paramIndex + 1;
                if (continueWriteBindParam(message, this.batchIndex, nextIndex, bindGroup, this.channelSInk)) {
                    nextBindGroup = handBindComplete(message, this.batchIndex, this.channelSInk);
                }
            } catch (Throwable e) {
                handleBindError(message, e, this.batchIndex, this.channelSInk);
            }
            if (nextBindGroup != null) {
                continueBindExecuteInEventLoop(this.channelSInk, this.batchIndex + 1);
            }

        }

        private void onErrorInEventLoop(Throwable t) {
            if (this.end) {
                return;
            }
            this.end = true;
            String msg = String.format("batch[%s] parameter[%s] Publisher error.", this.batchIndex, this.paramIndex);
            final Throwable error = new LongDataReadException(msg, t);
            this.error = error;
            handleBindError(this.message, error, this.batchIndex, this.channelSInk);
        }

        private void onNextInEventLoop(final Object obj) {
            if (this.end) {
                return;
            }
            if (!(obj instanceof byte[])) {
                this.end = true;
                String msg = String.format("batch[%s] parameter[%s] Publisher element isn't byte[]."
                        , this.batchIndex, this.paramIndex);
                cancelSubscribe();
                handleBindError(this.message, new SQLException(msg), this.batchIndex, this.channelSInk);
                return;
            }

            final byte[] bytes = (byte[]) obj;

            Throwable error = null;
            final ByteBuf message = this.message;
            if (message.maxWritableBytes() < bytes.length) {
                error = PgExceptions.tooLargeObject();
            } else if (this.binaryData || StandardCharsets.UTF_8.equals(this.clientCharset)) {
                message.writeBytes(bytes);
            } else {
                final byte[] writeBytes = new String(bytes, StandardCharsets.UTF_8).getBytes(this.clientCharset);
                if (message.maxWritableBytes() < writeBytes.length) {
                    error = PgExceptions.tooLargeObject();
                } else {
                    message.writeBytes(writeBytes);
                }
            }
            if (error != null) {
                this.error = error;
                stmtTask.addErrorToTask(error);
                cancelSubscribe();
                handleBindError(this.message, error, this.batchIndex, this.channelSInk);
            }

        }

        private void cancelSubscribe() {
            final Subscription subscription = this.subscription;
            if (subscription != null) {
                try {
                    subscription.cancel();
                } catch (Throwable e) {
                    // Subscription.cannel() shouldn't throw error.
                    LOG.debug("subscription cancel() throw error", e);
                }
            }
        }


    }


}
