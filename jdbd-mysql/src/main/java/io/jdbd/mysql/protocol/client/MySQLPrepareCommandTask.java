package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.vendor.conf.Properties;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * @see ComPreparedTask
 * @see MySQLCommandTask
 */
abstract class MySQLPrepareCommandTask extends MySQLCommandTask {

    private static final AtomicIntegerFieldUpdater<MySQLPrepareCommandTask> SEQUENCE_ID =
            AtomicIntegerFieldUpdater.newUpdater(MySQLPrepareCommandTask.class, "sequenceId");

    final MySQLTaskAdjutant adjutant;

    final int negotiatedCapability;

    final Properties<PropertyKey> properties;

    private volatile int sequenceId = -1;

    MySQLPrepareCommandTask(MySQLTaskAdjutant adjutant) {
        super(adjutant);
        this.negotiatedCapability = adjutant.obtainNegotiatedCapability();
        this.adjutant = adjutant;
        this.properties = adjutant.obtainHostInfo().getProperties();
    }


    public final int safelyUpdateSequenceId(final int sequenceId) {
        final int newSequenceId;
        if (sequenceId < 0) {
            newSequenceId = -1;
        } else {
            newSequenceId = sequenceId & 0XFF;
        }
        SEQUENCE_ID.set(this, newSequenceId);
        return newSequenceId;
    }

    public final int safelyObtainSequenceId() {
        return SEQUENCE_ID.get(this);
    }


    public final int safelyAddAndGetSequenceId() {
        return SEQUENCE_ID.updateAndGet(this, operand -> (++operand) & 0XFF);
    }


    static JdbdSQLException createSequenceIdError(int expected, ByteBuf cumulateBuffer) {
        return MySQLExceptions.createFatalIoException(
                (Throwable) null
                , "MySQL server row packet return sequence_id error,expected[%s] actual[%s]"
                , expected, PacketUtils.getInt1AsInt(cumulateBuffer, cumulateBuffer.readerIndex() - 1));
    }
}
