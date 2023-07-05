package io.jdbd.postgre.protocol.client;

import io.jdbd.env.Properties;
import io.jdbd.vendor.task.UnitTask;

abstract class PostgreUnitTask extends UnitTask<TaskAdjutant> {

    final TaskAdjutant adjutant;

    final Properties properties;

    PostgreUnitTask(final PgTask task) {
        super(task);
        this.adjutant = task.adjutant;
        this.properties = this.adjutant.obtainHost().getProperties();
    }


}
