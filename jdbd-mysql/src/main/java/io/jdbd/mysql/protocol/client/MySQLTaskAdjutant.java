package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.syntax.MySQLParser;
import io.jdbd.vendor.task.TaskAdjutant;

interface MySQLTaskAdjutant extends TaskAdjutant, ClientProtocolAdjutant, MySQLParser {

    int getServerStatus();

}
