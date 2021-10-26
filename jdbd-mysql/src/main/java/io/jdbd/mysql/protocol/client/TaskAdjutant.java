package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.authentication.AuthenticationPlugin;
import io.jdbd.mysql.session.SessionAdjutant;
import io.jdbd.mysql.syntax.MySQLParser;
import io.jdbd.vendor.task.ITaskAdjutant;

import java.nio.charset.Charset;
import java.util.Map;

interface TaskAdjutant extends ITaskAdjutant, ClientProtocolAdjutant, MySQLParser {

    int getServerStatus();

    /**
     * @see SessionAdjutant#pluginClassMap()
     */
    Map<String, Class<? extends AuthenticationPlugin>> obtainPluginMechanismMap();

    boolean isAuthenticated();

    MySQLParser sqlParser();

    Map<String, Charset> customCharsetMap();

    Map<String, MyCharset> nameCharsetMap();

    Map<Integer, Collation> idCollationMap();

    Map<String, Collation> nameCollationMap();

    boolean inTransaction();

    boolean isAutoCommit();

}
