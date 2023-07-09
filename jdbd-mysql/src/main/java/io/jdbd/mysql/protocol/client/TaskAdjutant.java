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

    ClientProtocolFactory getFactory();

    boolean isNoBackslashEscapes();

    /**
     * <p>
     * Beginning with MySQL 8.0.19, you can specify a time zone offset when inserting TIMESTAMP and DATETIME values into a table.
     * Datetime literals that include time zone offsets are accepted as parameter values by prepared statements.
     * </p>
     *
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/date-and-time-literals.html">Date and Time Literals</a>
     */
    boolean isSupportZoneOffset();

}
