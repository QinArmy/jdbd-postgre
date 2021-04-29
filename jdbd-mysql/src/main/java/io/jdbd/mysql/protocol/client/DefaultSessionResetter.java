package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.mysql.SQLMode;
import io.jdbd.mysql.Server;
import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.ServerVersion;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.stmt.Stmts;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.jdbd.mysql.util.MySQLTimeUtils;
import io.jdbd.result.ResultRow;
import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.util.SQLStates;
import org.qinarmy.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.*;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

final class DefaultSessionResetter implements SessionResetter {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultSessionResetter.class);

    static SessionResetter create(TaskAdjutant adjutant) {
        return new DefaultSessionResetter(adjutant);
    }


    private final Properties<PropertyKey> properties;

    private final TaskAdjutant adjutant;

    private final ConcurrentMap<Key, Object> configCacheMap = new ConcurrentHashMap<>(9);

    private DefaultSessionResetter(TaskAdjutant adjutant) {
        this.adjutant = adjutant;
        this.properties = adjutant.obtainHostInfo().getProperties();
    }

    @Override
    public Mono<Server> reset() {
        return Mono.defer(this::executeReset);
    }


    /*################################## blow private method ##################################*/

    private Mono<Server> executeReset() {
        this.configCacheMap.clear();

        return executeSetSessionVariables()//1.
                .then(configSessionCharset())//2.
                .then(configZoneOffsets())//3.
                .then(configSqlMode())//4.

                .then(initializeTransaction())//5.
                .then(cacheGlobalLocalInfileVariable())//6.
                .then(Mono.defer(this::createServer))//
                ;
    }


    private Mono<Server> createServer() {
        Mono<Server> mono;
        try {
            Server server = new DefaultServer(this.configCacheMap, this.properties);
            mono = Mono.just(server);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Database session[{}] reset success,{}"
                        , this.adjutant.obtainHandshakeV10Packet().getThreadId(), server);
            }
        } catch (Throwable e) {
            mono = Mono.error(MySQLExceptions.wrap(e));
        }
        return mono;
    }

    /**
     * execute config session variables in {@link PropertyKey#sessionVariables}
     *
     * @see #reset()
     */
    Mono<Void> executeSetSessionVariables() {
        String pairString = this.properties.getProperty(PropertyKey.sessionVariables);
        if (!MySQLStringUtils.hasText(pairString)) {
            return Mono.empty();
        }
        final String command = Commands.buildSetVariableCommand(pairString);
        if (LOG.isDebugEnabled() && !this.properties.getOrDefault(PropertyKey.paranoid, Boolean.class)) {
            LOG.debug("execute set session variables:{}", command);
        }
        return ComQueryTask.update(Stmts.stmt(command), this.adjutant)
                .then();

    }

    /**
     * config three session variables:
     * <u>
     * <li>{@link PropertyKey#characterEncoding}</li>
     * <li>{@link PropertyKey#connectionCollation}</li>
     * <li> {@link PropertyKey#characterSetResults}</li>
     * </u>
     *
     * @see #reset()
     * @see #configResultsCharset()
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/charset-connection.html">Connection Character Sets and Collations</a>
     */
    Mono<Void> configSessionCharset() {

        final CharsetMapping.Collation connectionCollation = tryGetConnectionCollation();

        final Charset clientCharset;
        final String namesCommand;

        if (connectionCollation == null) {
            final CharsetMapping.CustomCollation customCollation = tryGetConnectionCollationWithinCustom();
            if (customCollation == null) {
                Pair<CharsetMapping.MySQLCharset, Charset> clientPair = obtainClientMySQLCharset();
                namesCommand = String.format("SET NAMES '%s'", clientPair.getFirst().charsetName);
                clientCharset = clientPair.getSecond();
            } else {
                final Charset charset = CharsetMapping.getJavaCharsetByMySQLCharsetName(customCollation.charsetName);
                if (charset == null) {
                    String message = String.format("Not found java charset for %s[%s],that is unknown charset."
                            , PropertyKey.connectionCollation, customCollation.collationName);
                    return Mono.error(new JdbdSQLException(new SQLException(message, SQLStates.CONNECTION_EXCEPTION)));
                }
                namesCommand = String.format("SET NAMES '%s' COLLATE '%s'"
                        , customCollation.charsetName
                        , customCollation.collationName);
                clientCharset = charset;
            }
        } else {
            namesCommand = String.format("SET NAMES '%s' COLLATE '%s'"
                    , connectionCollation.mySQLCharset.charsetName
                    , connectionCollation.collationName);
            clientCharset = Charset.forName(connectionCollation.mySQLCharset.javaEncodingsUcList.get(0));
        }
        this.configCacheMap.put(Key.CHARSET_CLIENT, clientCharset);
        if (LOG.isDebugEnabled()) {
            LOG.debug("config session charset:{}", namesCommand);
        }
        return ComQueryTask.update(Stmts.stmt(namesCommand), this.adjutant)
                .then(Mono.defer(this::configResultsCharset));
    }


    /**
     * config {@link Key#ZONE_OFFSET_CLIENT} and {@link Key#ZONE_OFFSET_DATABASE}
     *
     * @see PropertyKey#cacheDefaultTimezone
     * @see PropertyKey#connectionTimeZone
     * @see #reset()
     */
    Mono<Void> configZoneOffsets() {
        final long utcEpochSecond = OffsetDateTime.now(ZoneOffset.UTC).toEpochSecond();
        String command = String.format("SELECT @@SESSION.time_zone as timeZone,DATE_FORMAT(FROM_UNIXTIME(%s),'%s') as databaseNow"
                , utcEpochSecond, "%Y-%m-%d %T");
        return ComQueryTask.query(Stmts.stmt(command), this.adjutant)
                .elementAt(0)
                .flatMap(resultRow -> handleDatabaseTimeZoneAndConnectionZone(resultRow, utcEpochSecond))
                ;
    }


    /**
     * @see #reset()
     */
    Mono<Void> configSqlMode() {
        String command = "SELECT @@SESSION.sql_mode";
        return ComQueryTask.query(Stmts.stmt(command), this.adjutant)
                .elementAt(0)
                .flatMap(this::appendSqlModeIfNeed)
                .then()
                ;

    }


    /**
     * <p>
     * must invoke after {@link #reset()}
     * </p>
     * <p>
     * do initialize ,must contain below operation:
     *     <ul>
     *         <li>{@code SET autocommit = 0}</li>
     *         <li>{@code SET SESSION TRANSACTION READ COMMITTED}</li>
     *         <li>more initializing operations</li>
     *     </ul>
     * </p>
     *
     * @see #reset()
     */
    Mono<Void> initializeTransaction() {
        final String autoCommitCommand = "SET autocommit = 1";
        final String isolationCommand = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ";
        return ComQueryTask.update(Stmts.stmt(autoCommitCommand), this.adjutant)
                .doOnSuccess(states -> LOG.debug("Command [{}] execute success.", autoCommitCommand))
                // blow 2 step
                .then(ComQueryTask.update(Stmts.stmt(isolationCommand), this.adjutant))
                .doOnSuccess(states -> LOG.debug("Command [{}] execute success.", isolationCommand))
                .then();
    }

    /**
     * @see #reset()
     */
    Mono<Void> cacheGlobalLocalInfileVariable() {
        // now only cache local_infile
        return ComQueryTask.query(Stmts.stmt("SELECT @@GLOBAL.local_infile"), this.adjutant)
                .elementAt(0)
                .doOnSuccess(this::storeGlobalLocalInfileVariable)
                .then();
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_local_infile">local_infile</a>
     */
    private void storeGlobalLocalInfileVariable(ResultRow row) {
        this.configCacheMap.put(Key.LOCAL_INFILE, row.getNonNull(0, Boolean.class));
    }

    /*################################## blow private method ##################################*/

    /**
     * config  {@link PropertyKey#characterSetResults} variables:
     *
     * @see #reset()
     * @see #configSessionCharset()
     */
    private Mono<Void> configResultsCharset() {
        final String charsetString = this.properties.getProperty(PropertyKey.characterSetResults, String.class);

        final Charset charsetResults;
        final String command;
        if (charsetString == null
                || charsetString.equalsIgnoreCase(Constants.NULL)) {
            command = "SET character_set_results = NULL";
            charsetResults = null;
        } else if (charsetString.equalsIgnoreCase("binary")
                || StandardCharsets.ISO_8859_1.name().equals(charsetString)
                || StandardCharsets.ISO_8859_1.aliases().contains(charsetString)) {
            command = "SET character_set_results = 'binary'";
            charsetResults = null;
        } else {
            CharsetMapping.MySQLCharset mySQLCharset;
            mySQLCharset = CharsetMapping.CHARSET_NAME_TO_CHARSET.get(charsetString.toLowerCase());
            if (mySQLCharset == null) {
                String message = String.format("No found MySQL charset[%s] fro Property[%s]"
                        , charsetString, PropertyKey.characterSetResults);
                return Mono.error(new JdbdSQLException(new SQLException(message, SQLStates.CONNECTION_EXCEPTION)));
            }
            command = String.format("SET character_set_results = '%s'", mySQLCharset.charsetName);
            charsetResults = Charset.forName(mySQLCharset.javaEncodingsUcList.get(0));
        }
        if (charsetResults == null) {
            this.configCacheMap.remove(Key.CHARSET_RESULTS);
        } else {
            this.configCacheMap.put(Key.CHARSET_RESULTS, charsetResults);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("config charset result:{}", command);
        }
        return ComQueryTask.update(Stmts.stmt(command), this.adjutant)
                .then();
    }

    /**
     * @see #configSqlMode()
     */
    private Mono<Void> appendSqlModeIfNeed(final ResultRow resultRow) {
        String sqlModeString = resultRow.getNonNull(0, String.class);
        final Set<String> sqlModeSet = MySQLStringUtils.spitAsSet(sqlModeString, ",");

        final Mono<Void> mono;
        if (!this.properties.getOrDefault(PropertyKey.timeTruncateFractional, Boolean.class)
                || sqlModeSet.contains(SQLMode.TIME_TRUNCATE_FRACTIONAL.name())) {
            this.configCacheMap.put(Key.SQL_MODE_SET, Collections.unmodifiableSet(sqlModeSet));
            mono = Mono.empty();
        } else {
            sqlModeSet.add(SQLMode.TIME_TRUNCATE_FRACTIONAL.name());
            StringBuilder commandBuilder = new StringBuilder("SET @@SESSION.sql_mode = '");
            int count = 0;
            for (String sqlMode : sqlModeSet) {
                if (count > 0) {
                    commandBuilder.append(",");
                }
                commandBuilder.append(sqlMode);
                count++;
            }
            commandBuilder.append("'");
            final Set<String> unmodifiableSet = Collections.unmodifiableSet(sqlModeSet);
            mono = ComQueryTask.update(Stmts.stmt(commandBuilder.toString()), this.adjutant)
                    .doOnSuccess(states -> this.configCacheMap.put(Key.SQL_MODE_SET, unmodifiableSet))
                    .then();
        }
        return mono;

    }


    /**
     * @see #configZoneOffsets()
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/time-zone-support.html#time-zone-variables">Time Zone Variables</a>
     */
    private Mono<Void> handleDatabaseTimeZoneAndConnectionZone(final ResultRow resultRow, final long utcEpochSecond) {
        // 1. handle query result.
        String databaseZoneText = resultRow.getNonNull("timeZone", String.class);
        final ZoneOffset zoneOffsetDatabase;
        if ("SYSTEM".equals(databaseZoneText)) {
            LocalDateTime dateTime = LocalDateTime.parse(resultRow.getNonNull("databaseNow", String.class)
                    , MySQLTimeUtils.MYSQL_DATETIME_FORMATTER);
            OffsetDateTime databaseNow = OffsetDateTime.of(dateTime, ZoneOffset.UTC);

            int totalSeconds = (int) (databaseNow.toEpochSecond() - utcEpochSecond);
            zoneOffsetDatabase = ZoneOffset.ofTotalSeconds(totalSeconds);
            this.configCacheMap.put(Key.ZONE_OFFSET_DATABASE, zoneOffsetDatabase);
        } else {
            try {
                ZoneId databaseZone = ZoneId.of(databaseZoneText, ZoneId.SHORT_IDS);
                zoneOffsetDatabase = MySQLTimeUtils.toZoneOffset(databaseZone);
                this.configCacheMap.put(Key.ZONE_OFFSET_DATABASE, zoneOffsetDatabase);
            } catch (DateTimeException e) {
                String message = String.format("MySQL time_zone[%s] cannot convert to %s ."
                        , databaseZoneText, ZoneId.class.getName());
                SQLException se = new SQLException(message, SQLStates.CONNECTION_EXCEPTION);
                return Mono.error(new JdbdSQLException(se));
            }
        }
        // 2. handle connectionTimeZone
        String connectionTimeZone = this.properties.getOrDefault(PropertyKey.connectionTimeZone);
        final ZoneOffset zoneOffsetClient;
        final ClientZoneMode clientZoneMode;
        if (Constants.LOCAL.equals(connectionTimeZone)) {
            zoneOffsetClient = MySQLTimeUtils.systemZoneOffset();
            clientZoneMode = ClientZoneMode.LOCAL;
        } else if ("SERVER".equals(connectionTimeZone)) {
            zoneOffsetClient = zoneOffsetDatabase;
            clientZoneMode = ClientZoneMode.SERVER;
        } else {
            try {
                zoneOffsetClient = MySQLTimeUtils.toZoneOffset(ZoneId.of(connectionTimeZone, ZoneId.SHORT_IDS));
                clientZoneMode = ClientZoneMode.OFFSET;
            } catch (DateTimeException e) {
                String message = String.format("Value[%s] of Property[%s] cannot convert to ZoneOffset."
                        , connectionTimeZone, PropertyKey.connectionTimeZone);
                SQLException se = new SQLException(message, SQLStates.CONNECTION_EXCEPTION);
                return Mono.error(new JdbdSQLException(se));
            }
        }
        this.configCacheMap.put(Key.CLIENT_ZONE_MODE, clientZoneMode);
        this.configCacheMap.put(Key.ZONE_OFFSET_CLIENT, zoneOffsetClient);
        return Mono.empty();
    }

    private Pair<CharsetMapping.MySQLCharset, Charset> obtainClientMySQLCharset() {
        final String charsetClient = properties.getProperty(PropertyKey.characterEncoding);

        final Pair<CharsetMapping.MySQLCharset, Charset> defaultPair;
        defaultPair = new Pair<>(
                CharsetMapping.CHARSET_NAME_TO_CHARSET.get(CharsetMapping.utf8mb4)
                , StandardCharsets.UTF_8
        );

        Pair<CharsetMapping.MySQLCharset, Charset> pair = null;
        if (!MySQLStringUtils.hasText(charsetClient)
                || !CharsetMapping.isUnsupportedCharsetClient(charsetClient)
                || StandardCharsets.UTF_8.name().equalsIgnoreCase(charsetClient)
                || StandardCharsets.UTF_8.aliases().contains(charsetClient)) {
            pair = defaultPair;
        } else {
            ServerVersion serverVersion = this.adjutant.obtainHandshakeV10Packet().getServerVersion();
            CharsetMapping.MySQLCharset mySQLCharset = CharsetMapping.getMysqlCharsetForJavaEncoding(
                    charsetClient, serverVersion);
            if (mySQLCharset != null) {
                pair = new Pair<>(mySQLCharset, Charset.forName(charsetClient));
            }
        }

        if (pair == null) {
            pair = defaultPair;
        }
        return pair;
    }

    @Nullable
    private CharsetMapping.Collation tryGetConnectionCollation() {

        String connectionCollation = this.properties.getProperty(PropertyKey.connectionCollation);
        if (!MySQLStringUtils.hasText(connectionCollation)) {
            return null;
        }
        CharsetMapping.Collation collationConnection;
        collationConnection = CharsetMapping.getCollationByName(connectionCollation);
        if (collationConnection != null
                && CharsetMapping.isUnsupportedCharsetClient(collationConnection.mySQLCharset.charsetName)) {
            collationConnection = CharsetMapping.INDEX_TO_COLLATION.get(CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4);
        }
        return collationConnection;
    }

    @Nullable
    private CharsetMapping.CustomCollation tryGetConnectionCollationWithinCustom() {

        String connectionCollation = this.properties.getProperty(PropertyKey.connectionCollation);

        CharsetMapping.CustomCollation customCollation = null;

        if (MySQLStringUtils.hasText(connectionCollation)) {
            Map<Integer, CharsetMapping.CustomCollation> map = this.adjutant.obtainCustomCollationMap();
            for (CharsetMapping.CustomCollation collation : map.values()) {
                if (collation.collationName.equals(connectionCollation)) {
                    customCollation = collation;
                    break;
                }
            }
        }
        return customCollation;

    }

    private enum Key {
        CHARSET_CLIENT,
        CHARSET_RESULTS,

        ZONE_OFFSET_DATABASE,
        ZONE_OFFSET_CLIENT,

        SQL_MODE_SET,
        LOCAL_INFILE,

        /**
         * @see ClientZoneMode
         */
        CLIENT_ZONE_MODE
    }

    private enum ClientZoneMode {
        LOCAL,
        SERVER,
        OFFSET
    }


    private static final class DefaultServer implements Server {

        private final Charset charsetClient;

        private final Charset charsetResults;

        private final ZoneOffset zoneOffsetDatabase;
        private final ZoneOffset zoneOffsetClient;

        private final Set<String> sqlModeSet;

        private final boolean supportLocalInfile;

        private final boolean cacheDefaultTimezone;

        private final ClientZoneMode clientZoneMode;

        @SuppressWarnings("unchecked")
        private DefaultServer(final ConcurrentMap<Key, Object> map, final Properties<PropertyKey> properties) {
            this.charsetClient = (Charset) Objects.requireNonNull(map.get(Key.CHARSET_CLIENT), Key.CHARSET_CLIENT.name());
            this.charsetResults = (Charset) map.get(Key.CHARSET_RESULTS);
            this.zoneOffsetDatabase = (ZoneOffset) Objects.requireNonNull(map.get(Key.ZONE_OFFSET_DATABASE), Key.ZONE_OFFSET_DATABASE.name());
            this.zoneOffsetClient = (ZoneOffset) Objects.requireNonNull(map.get(Key.ZONE_OFFSET_CLIENT), Key.ZONE_OFFSET_CLIENT.name());

            this.sqlModeSet = (Set<String>) Objects.requireNonNull(map.get(Key.SQL_MODE_SET), Key.SQL_MODE_SET.name());
            this.supportLocalInfile = (Boolean) Objects.requireNonNull(map.get(Key.LOCAL_INFILE), Key.LOCAL_INFILE.name());

            this.cacheDefaultTimezone = properties.getOrDefault(PropertyKey.cacheDefaultTimezone, Boolean.class);
            this.clientZoneMode = (ClientZoneMode) Objects.requireNonNull(map.get(Key.CLIENT_ZONE_MODE), Key.CLIENT_ZONE_MODE.name());
        }

        @Override
        public final String toString() {
            return new StringBuilder("DefaultServer{")
                    .append("\n charsetClient=").append(this.charsetClient)
                    .append("\n charsetResults=").append(this.charsetResults)
                    .append("\n zoneOffsetDatabase=").append(this.zoneOffsetDatabase)
                    .append("\n zoneOffsetClient=").append(this.zoneOffsetClient)
                    .append("\n clientZoneMode=").append(this.clientZoneMode)
                    .append("\n cacheDefaultTimezone=").append(this.cacheDefaultTimezone)
                    .append("\n sqlModeSet=").append(this.sqlModeSet)
                    .append("\n supportLocalInfile=").append(this.supportLocalInfile)
                    .append("\n}").toString();
        }

        @Override
        public final boolean containSqlMode(SQLMode sqlMode) {
            return this.sqlModeSet.contains(sqlMode.name());
        }

        @Override
        public final Charset obtainCharsetClient() {
            return this.charsetClient;
        }

        @Nullable
        @Override
        public final Charset obtainCharsetResults() {
            return this.charsetResults;
        }

        @Override
        public final ZoneOffset obtainZoneOffsetDatabase() {
            return this.zoneOffsetDatabase;
        }

        @Override
        public final ZoneOffset obtainZoneOffsetClient() {
            final ZoneOffset zoneOffset;
            if (this.clientZoneMode == ClientZoneMode.LOCAL && this.cacheDefaultTimezone) {
                zoneOffset = MySQLTimeUtils.systemZoneOffset();
            } else {
                zoneOffset = this.zoneOffsetClient;
            }
            return zoneOffset;
        }

        @Override
        public final boolean supportLocalInfile() {
            return this.supportLocalInfile;
        }


    }


}
