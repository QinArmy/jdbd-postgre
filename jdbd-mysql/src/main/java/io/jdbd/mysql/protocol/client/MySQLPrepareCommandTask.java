package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.vendor.conf.Properties;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * @see ComPreparedTask
 * @see MySQLCommandTask
 */
abstract class MySQLPrepareCommandTask extends MySQLCommandTask implements StatementTask {

    private static final AtomicIntegerFieldUpdater<MySQLPrepareCommandTask> SEQUENCE_ID =
            AtomicIntegerFieldUpdater.newUpdater(MySQLPrepareCommandTask.class, "safeSequenceId");


    final MySQLTaskAdjutant adjutant;

    final int negotiatedCapability;

    final Properties<PropertyKey> properties;

    private boolean useSafeSequenceId;

    private volatile int safeSequenceId = -1;

    MySQLPrepareCommandTask(MySQLTaskAdjutant adjutant) {
        super(adjutant);
        this.negotiatedCapability = adjutant.obtainNegotiatedCapability();
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
