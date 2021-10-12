package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.vendor.result.FluxResultSink;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Base class of All MySQL command phase communication task.
 *
 * @see ComQueryTask
 * @see QuitTask
 * @see MySQLPrepareCommandTask
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase.html">Command Phase</a>
 */
abstract class AbstractCommandTask extends MySQLTask implements StmtTask {

    final Logger log = LoggerFactory.getLogger(getClass());

    final FluxResultSink sink;

    final int negotiatedCapability;

    private int sequenceId = -1;

    private int resultIndex;

    private boolean downstreamCanceled;

    AbstractCommandTask(TaskAdjutant adjutant, FluxResultSink sink) {
        super(adjutant, sink::error);
        this.sink = sink;
        this.negotiatedCapability = adjutant.negotiatedCapability();
    }

    /*################################## blow StmtTask method ##################################*/

    @Override
    public final void addErrorToTask(Throwable error) {
        addError(error);
    }

    @Override
    public final TaskAdjutant adjutant() {
        return this.adjutant;
    }

    @Override
    public final void updateSequenceId(final int sequenceId) {
        if (sequenceId < 0) {
            this.sequenceId = -1;
        } else {
            this.sequenceId = sequenceId & 0xFF;
        }
    }


    @Override
    public final boolean readResultStateWithReturning(ByteBuf cumulateBuffer, Supplier<Integer> resultIndexes) {

        return false;
    }

    @Override
    public final int getAndIncrementResultIndex() {
        return this.resultIndex++;
    }

    @Override
    public final boolean isCanceled() {
        final boolean isCanceled;
        if (this.downstreamCanceled || hasError()) {
            isCanceled = true;
        } else if (this.sink.isCancelled()) {
            log.trace("Downstream cancel subscribe.");
            this.downstreamCanceled = isCanceled = true;
        } else {
            isCanceled = false;
        }
        log.trace("Read command response,isCanceled:{}", isCanceled);
        return isCanceled;
    }

    public final int obtainSequenceId() {
        return this.sequenceId;
    }


    public final int addAndGetSequenceId() {
        return ++this.sequenceId;
    }


    final void readErrorPacket(final ByteBuf cumulateBuffer) {
        final int payloadLength = Packets.readInt3(cumulateBuffer);
        updateSequenceId(Packets.readInt1AsInt(cumulateBuffer)); //  sequence_id
        final ErrorPacket error;
        error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                , this.negotiatedCapability, this.adjutant.obtainCharsetError());
        addError(MySQLExceptions.createErrorPacketException(error));
    }

    final void readUpdateResult(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        final int payloadLength = Packets.readInt3(cumulateBuffer);
        updateSequenceId(Packets.readInt1AsInt(cumulateBuffer));
        final OkPacket ok;
        ok = OkPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);
        serverStatusConsumer.accept(ok);
        // emit update result.
        this.sink.next(MySQLResultStates.fromUpdate(getAndIncrementResultIndex(), ok));
    }


    @Override
    protected final boolean canDecode(ByteBuf cumulateBuffer) {
        return Packets.hasOnePacket(cumulateBuffer);
    }

    static JdbdSQLException createSequenceIdError(int expected, ByteBuf cumulateBuffer) {
        return MySQLExceptions.createFatalIoException(
                (Throwable) null
                , "MySQL server row packet return sequence_id error,expected[%s] actual[%s]"
                , expected, Packets.getInt1AsInt(cumulateBuffer, cumulateBuffer.readerIndex() - 1));
    }

}
