package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.task.CommunicationTask;
import io.netty.buffer.ByteBuf;

/**
 * Base class of All MySQL command phase communication task.
 *
 * @see ComQueryTask
 * @see QuitTask
 * @see MySQLPrepareCommandTask
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase.html">Command Phase</a>
 */
abstract class MySQLCommandTask extends CommunicationTask<TaskAdjutant> {

    final TaskAdjutant adjutant;

    final int negotiatedCapability;

    final Properties<PropertyKey> properties;

    private int sequenceId = -1;

    MySQLCommandTask(TaskAdjutant adjutant) {
        super(adjutant);
        this.negotiatedCapability = adjutant.obtainNegotiatedCapability();
        this.adjutant = adjutant;
        this.properties = adjutant.obtainHostInfo().getProperties();
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

    @Override
    protected final boolean hasOnePacket(ByteBuf cumulateBuffer) {
        return PacketUtils.hasOnePacket(cumulateBuffer);
    }

    static JdbdSQLException createSequenceIdError(int expected, ByteBuf cumulateBuffer) {
        return MySQLExceptions.createFatalIoException(
                (Throwable) null
                , "MySQL server row packet return sequence_id error,expected[%s] actual[%s]"
                , expected, PacketUtils.getInt1AsInt(cumulateBuffer, cumulateBuffer.readerIndex() - 1));
    }

}
