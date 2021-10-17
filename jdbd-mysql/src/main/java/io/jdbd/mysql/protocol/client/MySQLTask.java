package io.jdbd.mysql.protocol.client;

import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.task.CommunicationTask;

import java.util.function.Consumer;

/**
 * Base class of All MySQL task.
 *
 * @see ComQueryTask
 * @see QuitTask
 * @see MySQLPrepareCommandStmtTask
 */
abstract class MySQLTask extends CommunicationTask<TaskAdjutant> {

    final Properties properties;

    MySQLTask(TaskAdjutant adjutant, Consumer<Throwable> errorConsumer) {
        super(adjutant, errorConsumer);
        this.properties = adjutant.obtainHostInfo().getProperties();
    }


}
