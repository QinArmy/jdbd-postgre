package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.authentication.AuthenticationPlugin;
import io.jdbd.mysql.session.SessionAdjutant;
import io.jdbd.mysql.syntax.MySQLParser;
import io.jdbd.vendor.task.ITaskAdjutant;

import java.util.Map;

interface TaskAdjutant extends ITaskAdjutant, ClientProtocolAdjutant, MySQLParser {

    int getServerStatus();

    /**
     * @see SessionAdjutant#obtainPluginClassMap()
     */
    Map<String, Class<? extends AuthenticationPlugin>> obtainPluginMechanismMap();

    boolean isAuthenticated();

    MySQLParser sqlParser();

}
