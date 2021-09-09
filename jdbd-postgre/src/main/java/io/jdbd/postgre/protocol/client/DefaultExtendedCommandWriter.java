package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.BindBatchStmt;
import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.syntax.PgStatement;
import io.jdbd.postgre.util.PgBinds;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.postgre.util.PgTimes;
import io.jdbd.stmt.LocalFileException;
import io.jdbd.stmt.LongDataReadException;
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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @see ExtendedQueryTask
 */
final class DefaultExtendedCommandWriter implements ExtendedCommandWriter {

    static DefaultExtendedCommandWriter create(ExtendedStmtTask stmtTask) throws SQLException {
        return new DefaultExtendedCommandWriter(stmtTask);
    }

    private static final Logger LOG = LoggerFactory.getLogger(DefaultExtendedCommandWriter.class);

    private final ExtendedStmtTask stmtTask;

    private final TaskAdjutant adjutant;

    private final ParamSingleStmt stmt;

    private final boolean oneShot;

    private final String statementName;

    private final String portalName;

    private final String replacedSql;

    private List<PgType> paramTypeList;

    private final int fetchSize;


    private DefaultExtendedCommandWriter(ExtendedStmtTask stmtTask) throws SQLException {
        this.adjutant = stmtTask.adjutant();
        this.stmt = stmtTask.getStmt();
        this.stmtTask = stmtTask;
        final PgStatement statement = this.adjutant.sqlParser().parse(this.stmt.getSql());
        if (isOneShotStmt(this.stmt)) {
            if (statement.getStaticSql().size() != 1) {
                throw PgExceptions.createBindCountNotMatchError(0, 0, getFirstBatchBindCount(this.stmt));
            }
            this.oneShot = true;
        } else {
            this.oneShot = false;
        }
        this.replacedSql = replacePlaceholder(statement);
        this.statementName = "";
        this.portalName = "";
        this.fetchSize = 0;

    }

    @Override
    public final boolean isOneShot() {
        return this.oneShot;
    }


    @Override
    public final boolean supportFetch() {
        return this.fetchSize > 0;
    }

    @Override
    public final boolean needClose() {
        return false;
    }

    @Nullable
    @Override
    public final CachePrepare getCache() {
        return null;
    }

    @Override
    public final String getReplacedSql() {
        return this.replacedSql;
    }

    @Override
    public final Publisher<ByteBuf> prepare() {
        if (this.oneShot) {
            throw new IllegalStateException("Current is  one shot");
        }
        final List<ByteBuf> messageList = new ArrayList<>(2);
        messageList.add(createParseMessage());    // Parse message
        appendDescribeMessage(messageList, true); // Describe message for statement
        appendSyncMessage(messageList);           // Sync message
        return Flux.fromIterable(messageList);
    }

    @Override
    public final Publisher<ByteBuf> executeOneShot() {
        final List<BindValue> bindGroup = obtainBindGroupForOneShot();
        if (bindGroup.size() > 0 || !this.oneShot) {
            throw new IllegalStateException("Not one shot");
        }
        final List<ByteBuf> messageList = new ArrayList<>(3);

        messageList.add(createParseMessage());    // Parse message
        messageList.add(createBindMessage(0, bindGroup.size())); // Bind message
        appendDescribeMessage(messageList, false);// Describe message for portal
        appendExecuteMessage(messageList);        // Execute message

        appendSyncMessage(messageList);           // Sync message
        return Flux.fromIterable(messageList);
    }

    @Override
    public final Publisher<ByteBuf> bindAndExecute() {
        if (this.paramTypeList != null) {
            throw new IllegalStateException("duplication execute.");
        }
        this.paramTypeList = this.stmtTask.getParamTypeList();
        return Flux.create(sink -> {
            if (this.adjutant.inEventLoop()) {
                continueBindExecuteInEventLoop(sink, 0);
            } else {
                this.adjutant.execute(() -> continueBindExecuteInEventLoop(sink, 0));
            }
        });
    }

    @Override
    public final Publisher<ByteBuf> fetch() {
        if (!PgStrings.hasText(this.portalName) || this.fetchSize <= 0) {
            throw new IllegalStateException("Not support fetch.");
        }
        List<ByteBuf> messageList = new ArrayList<>(1);
        appendExecuteMessage(messageList);
        return Flux.fromIterable(messageList);
    }

    @Override
    public final Publisher<ByteBuf> closeStatement() {
        final String name = this.statementName;
        if (!PgStrings.hasText(name)) {
            throw new IllegalStateException("No statement name");
        }
        final byte[] nameBytes = name.getBytes(this.adjutant.clientCharset());
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
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Parse</a>
     */
    private ByteBuf createParseMessage() {
        final Charset charset = this.adjutant.clientCharset();

        final ByteBuf message = this.adjutant.allocator().buffer(1024, Integer.MAX_VALUE);
        //  write Parse message
        message.writeByte(Messages.P);
        message.writeZero(Messages.LENGTH_BYTES); // placeholder of length
        final String statementName = this.statementName;
        if (PgStrings.hasText(statementName)) {
            message.writeBytes(statementName.getBytes(charset));
        }
        message.writeByte(Messages.STRING_TERMINATOR);

        message.writeBytes(this.replacedSql.getBytes(charset));
        message.writeByte(Messages.STRING_TERMINATOR);

        message.writeShort(0); //jdbd-post no support parameter oid in Parse message,@see /document/note/Q%A.md

        Messages.writeLength(message);
        return message;
    }


    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Execute</a>
     */
    private void appendExecuteMessage(List<ByteBuf> messageList) {
        final String portalName = this.portalName;
        final byte[] portalNameBytes;
        if (portalName.equals("")) {
            portalNameBytes = new byte[0];
        } else {
            portalNameBytes = portalName.getBytes(this.adjutant.clientCharset());
        }
        final int length = 9 + portalNameBytes.length, needCapacity = length + 1;
        final int messageSize = messageList.size();
        final ByteBuf message;
        if (messageSize > 0) {
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
        message.writeByte(Messages.E);
        message.writeInt(length);
        if (portalNameBytes.length > 0) {
            message.writeBytes(portalNameBytes);
        }
        message.writeByte(Messages.STRING_TERMINATOR);
        message.writeInt(this.fetchSize);
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

        final ByteBuf message;
        if (messageList.size() > 0) {
            final ByteBuf lastMessage = messageList.get(messageList.size() - 1);
            if (lastMessage.isReadOnly() || needCapacity > lastMessage.writableBytes()) {
                message = this.adjutant.allocator().buffer(needCapacity);
                messageList.add(message);
            } else {
                message = lastMessage;
            }
        } else {
            message = this.adjutant.allocator().buffer(needCapacity);
            messageList.add(message);
        }
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

        final int messageSize = messageList.size(), needCapacity = 5;
        final ByteBuf message;
        if (messageSize > 0) {
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
        writeSyncMessage(message);
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Sync</a>
     */
    private void writeSyncMessage(ByteBuf message) {
        message.writeByte(Messages.S);
        message.writeInt(Messages.LENGTH_BYTES);
    }


    /**
     * @see #executeOneShot()
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Bind</a>
     */
    private ByteBuf createBindMessage(final int batchIndex, final int bindCount)
            throws JdbdSQLException {

        final Charset clientCharset = this.adjutant.clientCharset();
        final ByteBuf message = this.adjutant.allocator().buffer(1024);
        message.writeByte(Messages.B);
        message.writeZero(Messages.LENGTH_BYTES);//placeholder of length
        // The name of the destination portal (an empty string selects the unnamed portal).
        final String portalName = this.portalName;
        if (!portalName.equals("")) {
            message.writeBytes(portalName.getBytes(clientCharset));
        }
        message.writeByte(Messages.STRING_TERMINATOR);
        // The name of the source prepared statement (an empty string selects the unnamed prepared statement).
        final String statementName = this.statementName;
        if (!statementName.equals("")) {
            message.writeBytes(statementName.getBytes(clientCharset));
        }
        message.writeByte(Messages.STRING_TERMINATOR);

        final List<PgType> paramTypeList = this.oneShot ? Collections.emptyList() : this.stmtTask.getParamTypeList();
        final int paramCount = paramTypeList.size();
        if (bindCount != paramCount) {
            throw PgExceptions.parameterCountMatch(batchIndex, paramCount, bindCount);
        }
        message.writeShort(paramCount); // The number of parameter format codes
        for (PgType type : paramTypeList) {
            message.writeShort(decideParameterFormatCode(type));
        }
        message.writeShort(paramCount); // The number of parameter values
        if (this.oneShot) { // one shot.
            message.writeShort(paramCount);
            Messages.writeLength(message);
        }
        return message;
    }

    /**
     * @see #createBindMessage(int, int)
     */
    private int decideParameterFormatCode(final PgType type) {
        final int formatCode;
        switch (type) {
            case SMALLINT:
            case INTEGER:
            case REAL:
            case DOUBLE:
            case OID:
            case BIGINT:
            case BYTEA:
            case BOOLEAN:
                formatCode = 1; // binary format code
                // only these  is binary format ,because postgre no document about binary format ,and postgre binary protocol not good
                break;
            default:
                formatCode = 0; // all array type is text format
        }
        return formatCode;
    }

    private String replacePlaceholder(PgStatement statement) {

        final List<String> staticSqlList = statement.getStaticSql();
        String sql;
        if (staticSqlList.size() == 1) {
            sql = statement.getSql();
        } else {
            final StringBuilder builder = new StringBuilder(statement.getSql().length() + staticSqlList.size());
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
    private List<BindValue> obtainBindGroupForOneShot() throws IllegalStateException {
        final ParamSingleStmt stmt = this.stmt;
        final List<BindValue> bindGroup;
        if (stmt instanceof BindStmt) {
            bindGroup = ((BindStmt) stmt).getBindGroup();
        } else if (stmt instanceof BindBatchStmt) {
            final List<List<BindValue>> groupList = ((BindBatchStmt) stmt).getGroupList();
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
            List<? extends ParamValue> nextBindGroup = getBindGroup(batchIndex);
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


    private void writeNonNullBindValue(final ByteBuf message, final int batchIndex, final PgType pgType
            , final ParamValue paramValue)
            throws SQLException, LocalFileException {
        switch (pgType) {
            case SMALLINT: {// binary format
                message.writeShort(PgBinds.bindNonNullToShort(batchIndex, pgType, paramValue));
            }
            break;
            case INTEGER: {// binary format
                message.writeInt(PgBinds.bindNonNullToInt(batchIndex, pgType, paramValue));
            }
            break;
            case OID:
            case BIGINT: {// binary format
                message.writeLong(PgBinds.bindNonNullToLong(batchIndex, pgType, paramValue));
            }
            break;
            case REAL: {// binary format
                final float value = PgBinds.bindNonNullToFloat(batchIndex, pgType, paramValue);
                message.writeInt(Float.floatToIntBits(value));
            }
            break;
            case DOUBLE: {// binary format
                final double value = PgBinds.bindNonNullToDouble(batchIndex, pgType, paramValue);
                message.writeLong(Double.doubleToLongBits(value));
            }
            break;
            case DECIMAL: {// text format
                final String text = PgBinds.bindNonNullToDecimal(batchIndex, pgType, paramValue)
                        .toPlainString();
                Messages.writeString(message, text, this.adjutant.clientCharset());
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
            case CHAR:
            case VARCHAR:
            case UUID: {
                final String text;
                text = PgBinds.bindNonNullToString(batchIndex, pgType, paramValue);
                Messages.writeString(message, text, this.adjutant.clientCharset());
            }
            break;
            case BIT:
            case VARBIT: {// text format
                final String bitString = PgBinds.bindNonNullToBit(batchIndex, pgType, paramValue);
                Messages.writeString(message, bitString, this.adjutant.clientCharset());
            }
            break;
            case BYTEA: {// binary format
                bindNonNullToBytea(message, batchIndex, pgType, paramValue);
            }
            break;
            case TIME: {// text format
                final String text;
                text = PgBinds.bindNonNullToLocalTime(batchIndex, pgType, paramValue)
                        .format(PgTimes.ISO_LOCAL_TIME_FORMATTER);
                Messages.writeString(message, text, this.adjutant.clientCharset());
            }
            break;
            case TIMETZ: {// text format
                final String text;
                text = PgBinds.bindNonNullToOffsetTime(batchIndex, pgType, paramValue)
                        .format(PgTimes.ISO_OFFSET_TIME_FORMATTER);
                Messages.writeString(message, text, this.adjutant.clientCharset());
            }
            break;
            case DATE: {// text format
                final String text;
                text = PgBinds.bindNonNullToLocalDate(batchIndex, pgType, paramValue)
                        .format(DateTimeFormatter.ISO_LOCAL_DATE);
                Messages.writeString(message, text, this.adjutant.clientCharset());
            }
            break;
            case TIMESTAMP: {// text format
                final String text;
                text = PgBinds.bindNonNullToLocalDateTime(batchIndex, pgType, paramValue)
                        .format(PgTimes.ISO_LOCAL_DATETIME_FORMATTER);
                Messages.writeString(message, text, this.adjutant.clientCharset());
            }
            break;
            case TIMESTAMPTZ: {// text format
                final String text;
                text = PgBinds.bindNonNullToOffsetDateTime(batchIndex, pgType, paramValue)
                        .format(PgTimes.ISO_OFFSET_DATETIME_FORMATTER);
                Messages.writeString(message, text, this.adjutant.clientCharset());
            }
            break;
            case INTERVAL: {// text format
                final String text;
                text = PgBinds.bindNonNullToInterval(batchIndex, pgType, paramValue);
                Messages.writeString(message, text, this.adjutant.clientCharset());
            }
            break;
            case BOOLEAN: {// binary format
                if (PgBinds.bindNonNullToBoolean(batchIndex, pgType, paramValue)) {
                    message.writeByte(1);
                } else {
                    message.writeByte(0);
                }
            }
            break;
            case POINT:
            case CIRCLE:
            case LINE:
            case PATH:
            case POLYGON:
            case LINE_SEGMENT:
            case BOX: // all geometry type is text format
            case XML:
            case JSON:
            case JSONB:
            case TEXT:
            case TSQUERY:
            case TSVECTOR: {// text format
                this.bindNonNullToLongString(message, batchIndex, pgType, paramValue);
            }
            break;
            case BOOLEAN_ARRAY:
            case SMALLINT_ARRAY:
            case INTEGER_ARRAY:
            case BIGINT_ARRAY:
            case REAL_ARRAY:
            case DOUBLE_ARRAY:
            case DECIMAL_ARRAY:
            case INTERVAL_ARRAY:
            case DATE_ARRAY:
            case UUID_ARRAY:
            case MONEY_ARRAY: {
                final String text;
                text = PgBinds.bindNonNullToArrayWithoutEscapes(batchIndex, pgType, paramValue);
                Messages.writeString(message, text, this.adjutant.clientCharset());
            }
            break;

            case BYTEA_ARRAY:
            case BIT_ARRAY:

            case TIMESTAMPTZ_ARRAY:
            case TIMESTAMP_ARRAY:
            case VARBIT_ARRAY:
            case TIMETZ_ARRAY:

            case POINT_ARRAY:
            case BOX_ARRAY:
            case LINE_ARRAY:
            case PATH_ARRAY:
            case CIRCLES_ARRAY:
            case JSONB_ARRAY:


            case OID_ARRAY:

            case TIME_ARRAY:
            case JSON_ARRAY:

            case CHAR_ARRAY:
            case XML_ARRAY:
            case CIDR_ARRAY:

            case INET_ARRAY:

            case TEXT_ARRAY:
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

            case REF_CURSOR_ARRAY:

            case VARCHAR_ARRAY:
                break;
            case UNSPECIFIED:
            case REF_CURSOR:
            default:
                throw PgExceptions.createUnexpectedEnumException(pgType);
        }
    }


    /**
     * @see #writeNonNullBindValue(ByteBuf, int, PgType, ParamValue)
     */
    private void bindNonNullToBytea(ByteBuf message, final int batchIndex, PgType pgType, ParamValue paramValue)
            throws LocalFileException, SQLException {
        if (decideParameterFormatCode(pgType) != 1) {
            throw new IllegalStateException("format code error.");
        }
        final Object nonNull = paramValue.getNonNull();
        final byte[] value;
        if (nonNull instanceof byte[]) {
            value = (byte[]) nonNull;
        } else if (nonNull instanceof BigDecimal) {
            value = ((BigDecimal) nonNull).toPlainString().getBytes(this.adjutant.clientCharset());
        } else if (nonNull instanceof Path) {
            writePathWithBinary(message, batchIndex, pgType, paramValue);
            return;
        } else {
            value = nonNull.toString().getBytes(this.adjutant.clientCharset());
        }
        if (message.maxWritableBytes() < value.length) {
            throw PgExceptions.beyondMessageLength(batchIndex, paramValue);
        }
        message.writeBytes(value);
    }


    /**
     * @see #writeNonNullBindValue(ByteBuf, int, PgType, ParamValue)
     */
    private void bindNonNullToLongString(ByteBuf message, final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException, LocalFileException {
        if (paramValue.getNonNull() instanceof Path) {
            writePathWithString(message, batchIndex, pgType, paramValue);
        } else {
            final String text = PgBinds.bindNonNullToString(batchIndex, pgType, paramValue);
            Messages.writeString(message, text, this.adjutant.clientCharset());
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
        } catch (IndexOutOfBoundsException e) {
            throw PgExceptions.beyondMessageLength(batchIndex, paramValue);
        } catch (Throwable e) {
            throw PgExceptions.localFileWriteError(batchIndex, pgType, paramValue, e);
        }
    }


    @Nullable
    private List<? extends ParamValue> getBindGroup(final int batchIndex) throws IllegalArgumentException {
        ParamSingleStmt stmt = this.stmt;
        if (stmt instanceof PrepareStmt) {
            stmt = ((PrepareStmt) stmt).getStmt();
        }
        final List<? extends ParamValue> bindGroup;
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
            final ParamBatchStmt<? extends ParamValue> batchStmt = (ParamBatchStmt<? extends ParamValue>) stmt;
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
            , List<? extends ParamValue> bindGroup, FluxSink<ByteBuf> channelSink) throws SQLException {

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
            final Object value = paramValue.get();
            if (value == null) {
                message.writeInt(-1); //  -1 indicates a NULL parameter value
                continue;
            }
            final PgType pgType = paramTypeList.get(paramIndex);

            if (value instanceof Publisher) {
                if (!pgType.supportPublisher()) {
                    throw PgExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
                }
                final ParameterSubscriber subscriber;
                subscriber = new ParameterSubscriber(message, batchIndex, paramIndex, pgType, channelSink);
                final Publisher<?> publisher = (Publisher<?>) value;
                publisher.subscribe(subscriber);
                break;  // break loop , async bind
            } else {
                valueLengthIndex = message.writerIndex();
                message.writeZero(4); // placeholder of parameter value length.
                writeNonNullBindValue(message, batchIndex, pgType, paramValue);
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
            // write result format
            message.writeShort(paramCount);
            for (PgType pgType : paramTypeList) {
                message.writeShort(decideParameterFormatCode(pgType));
            }
        }
        return bindFinish;

    }

    /**
     * @see #continueBindExecuteInEventLoop(FluxSink, int)
     * @see ParameterSubscriber#onCompleteInEventLoop()
     */
    @Nullable
    private List<? extends ParamValue> handBindComplete(final ByteBuf bindMessage, final int batchIndex
            , FluxSink<ByteBuf> channelSink) {


        final List<? extends ParamValue> nextBindGroup = getBindGroup(batchIndex + 1);

        Messages.writeLength(bindMessage); // write bind message length .

        final List<ByteBuf> messageList = new ArrayList<>(2);
        messageList.add(bindMessage);               // Bind message
        appendDescribeMessage(messageList, false);  // Describe message for portal
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

    private void handleBindError(ByteBuf message, Throwable error, final int batchIndex
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
        channelSInk.complete(); // don't emit error to netty channel
        if (batchIndex == 0) {
            // Task need end task.
            this.stmtTask.handleNoExecuteMessage();
        }
    }



    /*################################## blow private static method ##################################*/


    private static void writePathWithBinary(ByteBuf message, final int batchIndex, PgType pgType, ParamValue paramValue)
            throws LocalFileException {
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
            throw PgExceptions.localFileWriteError(batchIndex, pgType, paramValue, e);
        }
    }

    private static boolean isOneShotStmt(final ParamSingleStmt stmt) {
        final boolean oneShot;
        if (stmt instanceof BindStmt) {
            oneShot = ((BindStmt) stmt).getBindGroup().isEmpty();
        } else if (stmt instanceof BindBatchStmt) {
            final List<List<BindValue>> groupList = ((BindBatchStmt) stmt).getGroupList();
            oneShot = groupList.size() == 1 && groupList.get(0).isEmpty();
        } else {
            oneShot = false;
        }
        return oneShot;
    }

    private static int getFirstBatchBindCount(final ParamSingleStmt stmt) {
        final int bindCount;
        if (stmt instanceof ParamStmt) {
            bindCount = ((ParamStmt) stmt).getBindGroup().size();
        } else {
            final ParamBatchStmt<? extends ParamValue> batchStmt = (ParamBatchStmt<? extends ParamValue>) stmt;
            if (batchStmt.getGroupList().isEmpty()) {
                bindCount = 0;
            } else {
                bindCount = batchStmt.getGroupList().get(0).size();
            }
        }
        return bindCount;
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
            message.writeZero(Messages.LENGTH_BYTES); // placeholder parameter value length

            this.message = message;
            this.batchIndex = batchIndex;
            this.paramIndex = paramIndex;
            this.channelSInk = channelSInk;

            this.adjutant = DefaultExtendedCommandWriter.this.adjutant;
            this.binaryData = pgType.supportBinaryPublisher();
            this.clientCharset = adjutant.clientCharset();
        }

        @Override
        public final void onSubscribe(Subscription s) {
            this.subscription = s;
            s.request(Long.MAX_VALUE);
        }

        @Override
        public final void onNext(final Object obj) {
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
        public final void onError(final Throwable t) {
            if (this.adjutant.inEventLoop()) {
                onErrorInEventLoop(t);
            } else {
                this.adjutant.execute(() -> onErrorInEventLoop(t));
            }
        }

        @Override
        public final void onComplete() {
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
            List<? extends ParamValue> nextBindGroup = null;
            final ByteBuf message = this.message;
            try {
                List<? extends ParamValue> bindGroup = getBindGroup(this.batchIndex);
                if (bindGroup == null) {
                    String msg = String.format("batchIndex[%s] not find any bind.", this.batchIndex);
                    throw new IllegalStateException(msg);
                }

                // write value length
                final int valueEndIndex = message.writerIndex();
                message.writerIndex(this.valueLengthIndex);
                message.writeInt(valueEndIndex - this.valueLengthIndex - Messages.LENGTH_BYTES);
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
