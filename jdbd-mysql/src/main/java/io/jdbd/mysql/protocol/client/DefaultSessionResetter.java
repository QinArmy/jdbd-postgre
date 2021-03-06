package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.MultiResults;
import io.jdbd.ResultRow;
import io.jdbd.mysql.SQLMode;
import io.jdbd.mysql.Server;
import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.ServerVersion;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLNumbers;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.jdbd.mysql.util.MySQLTimeUtils;
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
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

final class DefaultSessionResetter implements SessionResetter {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultSessionResetter.class);

    static SessionResetter create(MySQLTaskAdjutant adjutant) {
        return new DefaultSessionResetter(adjutant);
    }


    private final Properties<PropertyKey> properties;

    private final MySQLTaskAdjutant adjutant;

    private final AtomicReference<Charset> charsetClient = new AtomicReference<>(null);

    private final AtomicReference<Charset> charsetResults = new AtomicReference<>(null);

    private final AtomicReference<ZoneOffset> zoneOffsetDatabase = new AtomicReference<>(null);

    private final AtomicReference<ZoneOffset> zoneOffsetClient = new AtomicReference<>(null);

    private final AtomicReference<Set<SQLMode>> sqlModeSet = new AtomicReference<>(null);

    private DefaultSessionResetter(MySQLTaskAdjutant adjutant) {
        this.adjutant = adjutant;
        this.properties = adjutant.obtainHostInfo().getProperties();
    }

    @Override
    public Mono<Server> reset() {
        return executeSetSessionVariables()//1.
                .then(Mono.defer(this::configSessionCharset))//2.
                .then(Mono.defer(this::configZoneOffsets))//3.
                .then(Mono.defer(this::configSqlMode))//4.

                .then(Mono.defer(this::initializeTransaction))//5.
                .then(Mono.defer(this::createServer))//
                ;

    }


    /*################################## blow private method ##################################*/


    private Mono<Server> createServer() {

        Charset charsetClient = this.charsetClient.get();
        Charset charsetResults = this.charsetResults.get();
        ZoneOffset zoneOffsetDatabase = this.zoneOffsetDatabase.get();
        ZoneOffset zoneOffsetClient = this.zoneOffsetClient.get();

        Set<SQLMode> sqlModeSet = this.sqlModeSet.get();

        Mono<Server> mono;
        final String message;
        if (charsetClient == null) {
            message = String.format("%s config failure.", PropertyKey.characterEncoding);
        } else if (zoneOffsetDatabase == null) {
            message = "obtain database time_zone failure";
        } else if (zoneOffsetClient == null) {
            message = String.format("%s config failure.", PropertyKey.connectionTimeZone);
        } else if (sqlModeSet == null) {
            message = "obtain database sql_mode failure";
        } else {
            message = null;
        }

        if (message == null) {
            assert charsetClient != null;
            assert zoneOffsetClient != null;
            mono = Mono.just(new DefaultServer(charsetClient, charsetResults, zoneOffsetDatabase, zoneOffsetClient
                    , sqlModeSet));
        } else {
            mono = Mono.error(new JdbdSQLException(new SQLException(message, SQLStates.CONNECTION_EXCEPTION
                    , MySQLNumbers.CR_UNKNOWN_ERROR)));
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
        String command = Commands.buildSetVariableCommand(pairString);
        if (LOG.isDebugEnabled() && !this.properties.getOrDefault(PropertyKey.paranoid, Boolean.class)) {
            LOG.debug("execute set session variables:{}", command);
        }
        return ComQueryTask.update(command, this.adjutant)
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
        this.charsetClient.set(clientCharset);
        if (LOG.isDebugEnabled()) {
            LOG.debug("config session charset:{}", namesCommand);
        }
        return ComQueryTask.update(namesCommand, this.adjutant)
                .then(Mono.defer(this::configResultsCharset));
    }


    /**
     * config {@link #zoneOffsetClient} and {@link #zoneOffsetDatabase}
     *
     * @see #reset()
     */
    Mono<Void> configZoneOffsets() {
        final long utcEpochSecond = OffsetDateTime.now(ZoneOffset.UTC).toEpochSecond();
        String command = String.format("SELECT @@SESSION.time_zone as timeZone,DATE_FORMAT(FROM_UNIXTIME(%s),'%s') as databaseNow"
                , utcEpochSecond, "%Y-%m-%d %T");
        return ComQueryTask.query(command, MultiResults.EMPTY_CONSUMER, this.adjutant)
                .elementAt(0)
                .flatMap(resultRow -> handleDatabaseTimeZoneAndConnectionZone(resultRow, utcEpochSecond))
                ;
    }


    /**
     * @see #reset()
     */
    Mono<Void> configSqlMode() {
        String command = "SELECT @@SESSION.sql_mode";
        return ComQueryTask.query(command, MultiResults.EMPTY_CONSUMER, this.adjutant)
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
        return ComQueryTask.update(autoCommitCommand, this.adjutant)
                .doOnSuccess(states -> LOG.debug("Command [{}] execute success.", autoCommitCommand))
                // blow 2 step
                .flatMap(resultStates -> ComQueryTask.update(isolationCommand, this.adjutant))
                .doOnSuccess(states -> LOG.debug("Command [{}] execute success.", isolationCommand))
                .then()
                ;
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
            mySQLCharset = CharsetMapping.CHARSET_NAME_TO_CHARSET.get(charsetString);
            if (mySQLCharset == null) {
                String message = String.format("No found MySQL charset fro Property %s[%s]"
                        , PropertyKey.characterSetResults, charsetString);
                return Mono.error(new JdbdSQLException(new SQLException(message, SQLStates.CONNECTION_EXCEPTION)));
            }
            command = String.format("SET character_set_results = '%s'", mySQLCharset.charsetName);
            charsetResults = Charset.forName(mySQLCharset.javaEncodingsUcList.get(0));
        }
        this.charsetResults.set(charsetResults);

        if (LOG.isDebugEnabled()) {
            LOG.debug("config charset result:{}", command);
        }
        return ComQueryTask.update(command, this.adjutant)
                .then();
    }

    /**
     * @see #configSqlMode()
     */
    private Mono<Void> appendSqlModeIfNeed(final ResultRow resultRow) {
        String sqlModeString = resultRow.getRequiredObject(0, String.class);
        List<String> sqlModeValueList = MySQLStringUtils.spitAsList(sqlModeString, ",");

        final List<SQLMode> sqlModeList;
        if (sqlModeValueList.isEmpty()) {
            sqlModeList = new ArrayList<>();
        } else {
            sqlModeList = new ArrayList<>(sqlModeValueList.size());

            for (String sqlModeValue : sqlModeValueList) {
                sqlModeList.add(SQLMode.valueOf(sqlModeValue));
            }

        }

        final Mono<Void> mono;
        if (sqlModeList.contains(SQLMode.TIME_TRUNCATE_FRACTIONAL)) {
            this.sqlModeSet.set(EnumSet.copyOf(sqlModeList));
            mono = Mono.empty();
        } else {
            sqlModeList.add(SQLMode.TIME_TRUNCATE_FRACTIONAL);
            StringBuilder commandBuilder = new StringBuilder("SET @@SESSION.sql_mode = '");
            int count = 0;
            for (SQLMode sqlMode : sqlModeList) {
                if (count > 0) {
                    commandBuilder.append(",");
                }
                commandBuilder.append(sqlMode.name());
                count++;
            }
            commandBuilder.append("'");

            mono = ComQueryTask.update(commandBuilder.toString(), this.adjutant)
                    .doOnSuccess(states -> this.sqlModeSet.set(EnumSet.copyOf(sqlModeList)))
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
        String databaseZoneText = resultRow.getRequiredObject("timeZone", String.class);
        final ZoneOffset zoneOffsetDatabase;
        if ("SYSTEM".equals(databaseZoneText)) {
            LocalDateTime dateTime = resultRow.getRequiredObject("databaseNow", LocalDateTime.class);
            OffsetDateTime databaseNow = OffsetDateTime.of(dateTime, ZoneOffset.UTC);

            int totalSeconds = (int) (databaseNow.toEpochSecond() - utcEpochSecond);
            zoneOffsetDatabase = ZoneOffset.ofTotalSeconds(totalSeconds);
            this.zoneOffsetDatabase.set(zoneOffsetDatabase);
        } else {
            try {
                ZoneId databaseZone = ZoneId.of(databaseZoneText, ZoneId.SHORT_IDS);
                zoneOffsetDatabase = MySQLTimeUtils.toZoneOffset(databaseZone);
                this.zoneOffsetDatabase.set(zoneOffsetDatabase);
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
        if (Constants.LOCAL.equals(connectionTimeZone)) {
            zoneOffsetClient = MySQLTimeUtils.systemZoneOffset();
        } else if ("SERVER".equals(connectionTimeZone)) {
            zoneOffsetClient = zoneOffsetDatabase;
        } else {
            try {
                zoneOffsetClient = MySQLTimeUtils.toZoneOffset(ZoneId.of(connectionTimeZone, ZoneId.SHORT_IDS));
            } catch (DateTimeException e) {
                String message = String.format("Value[%s] of Property[%s] cannot convert to ZoneOffset."
                        , connectionTimeZone, PropertyKey.connectionTimeZone);
                SQLException se = new SQLException(message, SQLStates.CONNECTION_EXCEPTION);
                return Mono.error(new JdbdSQLException(se));
            }
        }
        this.zoneOffsetClient.set(zoneOffsetClient);
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


    private static final class DefaultServer implements Server {

        private final Charset charsetClient;

        private final Charset charsetResults;

        private final ZoneOffset zoneOffsetDatabase;
        private final ZoneOffset zoneOffsetClient;

        private final Set<SQLMode> sqlModeSet;

        private DefaultServer(Charset charsetClient, @Nullable Charset charsetResults
                , ZoneOffset zoneOffsetDatabase, ZoneOffset zoneOffsetClient
                , Set<SQLMode> sqlModeSet) {
            this.charsetClient = charsetClient;
            this.charsetResults = charsetResults;
            this.zoneOffsetDatabase = zoneOffsetDatabase;
            this.zoneOffsetClient = zoneOffsetClient;
            this.sqlModeSet = sqlModeSet;
        }

        @Override
        public boolean containSqlMode(SQLMode sqlMode) {
            return this.sqlModeSet.contains(sqlMode);
        }

        @Override
        public Charset obtainCharsetClient() {
            return this.charsetClient;
        }

        @Nullable
        @Override
        public Charset obtainCharsetResults() {
            return this.charsetResults;
        }

        @Override
        public ZoneOffset obtainZoneOffsetDatabase() {
            return this.zoneOffsetDatabase;
        }

        @Override
        public ZoneOffset obtainZoneOffsetClient() {
            return this.zoneOffsetClient;
        }


    }


}
