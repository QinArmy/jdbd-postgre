package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.vendor.conf.Properties;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;

/**
 * @see ComPreparedTask
 * @see AbstractCommandTask
 */
abstract class MySQLPrepareCommandStmtTask extends AbstractCommandTask implements PrepareStmtTask {

    private static final AtomicIntegerFieldUpdater<MySQLPrepareCommandStmtTask> SEQUENCE_ID =
            AtomicIntegerFieldUpdater.newUpdater(MySQLPrepareCommandStmtTask.class, "safeSequenceId");


    final TaskAdjutant adjutant;

    final int negotiatedCapability;

    final Properties<MyKey> properties;

    private boolean useSafeSequenceId;

    private volatile int safeSequenceId = -1;

    MySQLPrepareCommandStmtTask(TaskAdjutant adjutant, Consumer<Throwable> errorConsumer) {
        super(adjutant, errorConsumer);
        this.negotiatedCapability = adjutant.negotiatedCapability();
        this.adjutant = adjutant;
        this.properties = adjutant.obtainHostInfo().getProperties();
    }

    @Override
    public final int safelyAddAndGetSequenceId() {
        return this.useSafeSequenceId
                ? SEQUENCE_ID.updateAndGet(this, operand -> (++operand) & 0XFF)
                : addAndGetSequenceId();
    }

    @Override
    public final void startSafeSequenceId() {
        if (!this.useSafeSequenceId) {
            synchronized (this) {
                SEQUENCE_ID.set(this, obtainSequenceId());
                this.useSafeSequenceId = true;
            }
        }

    }

    @Override
    public final void endSafeSequenceId() {
        if (this.useSafeSequenceId) {
            synchronized (this) {
                updateSequenceId(SEQUENCE_ID.get(this));
                this.useSafeSequenceId = false;
            }
        }

    }


}