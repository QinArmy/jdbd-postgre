package io.jdbd.postgre.protocol.client;

import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.task.UnitTask;

abstract class PostgreUnitTask extends UnitTask<TaskAdjutant> {


    final Properties properties;

    PostgreUnitTask(PgTask task) {
        super(task);
        this.properties = this.adjutant.obtainHost().getProperties();
    }


}
