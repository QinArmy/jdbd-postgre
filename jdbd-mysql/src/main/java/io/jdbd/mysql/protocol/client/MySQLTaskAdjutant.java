package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.authentication.AuthenticationPlugin;
import io.jdbd.mysql.session.MySQLSessionAdjutant;
import io.jdbd.mysql.syntax.MySQLParser;
import io.jdbd.vendor.task.TaskAdjutant;

import java.util.Map;

interface MySQLTaskAdjutant extends TaskAdjutant, ClientProtocolAdjutant, MySQLParser {

    int getServerStatus();

    /**
     * @see MySQLSessionAdjutant#obtainPluginClassMap()
     */
    Map<String, Class<? extends AuthenticationPlugin>> obtainPluginMechanismMap();

}
