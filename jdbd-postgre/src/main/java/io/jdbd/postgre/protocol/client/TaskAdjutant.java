package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.config.PostgreHost;
import io.jdbd.vendor.task.ITaskAdjutant;

interface TaskAdjutant extends ITaskAdjutant {


    PostgreHost obtainHost();

}
