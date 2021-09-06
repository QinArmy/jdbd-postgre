package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.BindBatchStmt;
import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.vendor.stmt.ParamSingleStmt;
import io.jdbd.vendor.stmt.ParamValue;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @see ExtendedQueryTask
 */
final class DefaultExtendedCommandWriter implements ExtendedCommandWriter {

    static DefaultExtendedCommandWriter create(ExtendedStmtTask stmtTask) {
        return null;
    }

    private final ExtendedStmtTask stmtTask;

    private final TaskAdjutant adjutant;

    private final ParamSingleStmt stmt;

    private final boolean oneShot;

    private final String statementName;

    private final String portalName;

    private final String replacedSql;

    private final int fetchSize;


    private DefaultExtendedCommandWriter(ExtendedStmtTask stmtTask) throws SQLException {
        this.adjutant = stmtTask.adjutant();
        this.stmt = stmtTask.getStmt();
        this.oneShot = isOneShotStmt(this.stmt);
        this.stmtTask = stmtTask;
        this.replacedSql = replacePlaceholder(this.stmt.getSql());
        this.statementName = "";
        this.portalName = "";
        this.fetchSize = 0;

    }

    @Override
    public final boolean isOneShot() {
        return this.oneShot;
    }

    @Override
    public final boolean hasCache() {
        return false;
    }

    @Override
    public final boolean supportFetch() {
        return this.fetchSize > 0;
    }

    @Override
    public final boolean needClose() {
        return false;
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
        messageList.add(createBindMessage(0, bindGroup)); // Bind message
        appendDescribeMessage(messageList, false);// Describe message for portal
        appendExecuteMessage(messageList);        // Execute message

        appendSyncMessage(messageList);           // Sync message
        return Flux.fromIterable(messageList);
    }

    @Override
    public final Publisher<Iterable<ByteBuf>> bindExecute(ParamSingleStmt stmt) {
        return null;
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
        return Mono.just(createCloseStatementMessage());
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
        if (PgStrings.hasText(portalName)) {
            portalNameBytes = portalName.getBytes(this.adjutant.clientCharset());
        } else {
            portalNameBytes = new byte[0];
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
        message.writeBytes(nameBytes);
        message.writeByte(Messages.STRING_TERMINATOR);

    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Sync</a>
     */
    private void appendSyncMessage(final List<ByteBuf> messageList) {

        final int messageSize = messageList.size();
        final ByteBuf message;
        if (messageSize > 0) {
            final ByteBuf lastMessage = messageList.get(messageSize - 1);
            if (lastMessage.isReadOnly() || lastMessage.writableBytes() < 5) {
                message = this.adjutant.allocator().buffer(5);
                messageList.add(message);
            } else {
                message = lastMessage;
            }
        } else {
            message = this.adjutant.allocator().buffer(5);
            messageList.add(message);
        }
        message.writeByte(Messages.S);
        message.writeInt(0);

    }

    private ByteBuf createCloseStatementMessage() {
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

        return message;
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Bind</a>
     */
    private ByteBuf createBindMessage(final int batchIndex, final List<? extends ParamValue> bindGroup)
            throws JdbdSQLException {

        final Charset clientCharset = this.adjutant.clientCharset();
        final ByteBuf message = this.adjutant.allocator().buffer(1024);
        message.writeByte(Messages.B);
        message.writeZero(Messages.LENGTH_BYTES);//placeholder of length
        // The name of the destination portal (an empty string selects the unnamed portal).
        final String portalName = this.portalName;
        if (portalName != null) {
            message.writeBytes(portalName.getBytes(clientCharset));
        }
        message.writeByte(Messages.STRING_TERMINATOR);
        // The name of the source prepared statement (an empty string selects the unnamed prepared statement).
        final String statementName = this.statementName;
        if (statementName != null) {
            message.writeBytes(statementName.getBytes(clientCharset));
        }
        message.writeByte(Messages.STRING_TERMINATOR);

        final List<PgType> paramTypeList = this.stmtTask.getParamTypeList();
        final int paramCount = paramTypeList.size();
        if (bindGroup.size() != paramCount) {
            throw PgExceptions.parameterCountMatch(batchIndex, paramCount, bindGroup.size());
        }
        message.writeShort(paramCount); // The number of parameter format codes
        for (PgType type : paramTypeList) {
            message.writeShort(decideParameterFormatCode(type));
        }
        message.writeShort(paramCount); // The number of parameter values
        return message;
    }

    /**
     * @see #createBindMessage(int, List)
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

    private String replacePlaceholder(final String originalSql) throws SQLException {

        final List<String> staticSqlList = this.adjutant.sqlParser().parse(originalSql).getStaticSql();
        String sql;
        if (staticSqlList.size() == 1) {
            sql = originalSql;
        } else {
            final StringBuilder builder = new StringBuilder(originalSql.length() + staticSqlList.size());
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

    private List<BindValue> obtainBindGroupForOneShot() {
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



    /*################################## blow private static method ##################################*/

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


    private static final class BindCommandWriter {


    }


}
