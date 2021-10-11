package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.vendor.result.FluxResultSink;
import io.netty.buffer.ByteBuf;

import java.util.function.Consumer;

/**
 * Base class of All MySQL command phase communication task.
 *
 * @see ComQueryTask
 * @see QuitTask
 * @see MySQLPrepareCommandTask
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase.html">Command Phase</a>
 */
abstract class CommandTask extends MySQLTask {

    final FluxResultSink sink;

    final int negotiatedCapability;

    private int sequenceId = -1;

    private int resultIndex;

    CommandTask(TaskAdjutant adjutant, FluxResultSink sink) {
        super(adjutant, sink::error);
        this.sink = sink;
        this.negotiatedCapability = adjutant.negotiatedCapability();
    }

    public final int updateSequenceId(final int sequenceId) {
        final int newSequenceId;
        if (sequenceId < 0) {
            newSequenceId = -1;
        } else {
            newSequenceId = sequenceId & 0xFF;
        }
        this.sequenceId = newSequenceId;
        return newSequenceId;
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
        this.sink.next(MySQLResultStates.from(this.resultIndex++, ok));
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
