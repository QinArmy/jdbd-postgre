package io.jdbd.mysql.protocol.client;

import io.jdbd.ResultRow;
import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.MySQLPacket;
import io.jdbd.mysql.protocol.ServerVersion;
import io.jdbd.mysql.protocol.conf.HostInfo;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.protocol.conf.PropertyDefinitions;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.jdbd.mysql.util.MySQLTimeUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import org.qinarmy.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

final class ClientConnectionProtocolImpl extends AbstractClientProtocol
        implements ClientConnectionProtocol {

    private static final Logger LOG = LoggerFactory.getLogger(ClientConnectionProtocolImpl.class);


    static Mono<ClientConnectionProtocolImpl> create(HostInfo hostInfo, EventLoopGroup eventLoopGroup) {
        return DefaultCommTaskExecutor.create(hostInfo, eventLoopGroup)
                .map(commTaskExecutor -> {
                    ClientConnectionProtocolImpl protocol = new ClientConnectionProtocolImpl(hostInfo, commTaskExecutor);
                    DefaultCommTaskExecutor.setProtocolAdjutant(commTaskExecutor, protocol);
                    return protocol;
                })
                ;
    }

    private static final String NONE = "none";

    static final String CHARACTER_SET_CLIENT = "character_set_client";
    static final String CHARACTER_SET_RESULTS = "character_set_results";
    static final String COLLATION_CONNECTION = "collation_connection";


    /**
     * below {@code xxx_PHASE } representing connection phase.
     *
     * @see #connectionPhase
     */
    private static final int HANDSHAKE_PHASE = 0;
    private static final int SSL_PHASE = 1;
    private static final int AUTHENTICATION_PHASE = 2;
    private static final int CONFIGURE_SESSION_PHASE = 3;

    private static final int INITIALIZING_PHASE = 4;
    private static final int COMMAND_PHASE = 5;


    private final DefaultCommTaskExecutor commTaskExecutor;

    private final AtomicReference<HandshakeV10Packet> handshakeV10Packet = new AtomicReference<>(null);

    /**
     * connection phase client charset,will send to server by {@code Protocol::HandshakeResponse41}.
     */
    private final Charset handshakeCharset;

    private final AtomicReference<Integer> negotiatedCapability = new AtomicReference<>(null);

    private final AtomicInteger connectionPhase = new AtomicInteger(HANDSHAKE_PHASE);

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/charset-connection.html"> character_set_client system variable</a>
     * @see PropertyKey#characterEncoding
     */
    private final AtomicReference<Charset> charsetClient = new AtomicReference<>(null);

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/charset-connection.html"> character_set_results system variable</a>
     * @see PropertyKey#characterSetResults
     */
    private final AtomicReference<Charset> charsetResults = new AtomicReference<>(null);

    private final AtomicReference<Map<Integer, CharsetMapping.CustomCollation>> customCollationMap = new AtomicReference<>(null);

    private final AtomicReference<Set<String>> customCharsetNameSet = new AtomicReference<>(null);

    private final AtomicReference<ZoneOffset> zoneOffsetDatabase = new AtomicReference<>(null);

    private final AtomicReference<ZoneOffset> zoneOffsetClient = new AtomicReference<>(null);


    private ClientConnectionProtocolImpl(HostInfo hostInfo, DefaultCommTaskExecutor commTaskExecutor) {
        super(hostInfo, commTaskExecutor.getAdjutant());

        this.commTaskExecutor = commTaskExecutor;
        this.handshakeCharset = this.properties.getProperty(PropertyKey.characterEncoding
                , Charset.class, StandardCharsets.UTF_8);
    }


    @Override
    public Mono<Void> authenticateAndInitializing() {
        return receiveHandshake()
                .then(Mono.defer(this::sslNegotiate))
                .then(Mono.defer(this::authenticate))
                .then(Mono.defer(this::configureSession))
                .then(Mono.defer(this::initialize))
                ;
    }

    /*################################## blow ClientProtocolAdjutant method ##################################*/

    @Override
    public ByteBuf createPacketBuffer(int initialPayloadCapacity) {
        return this.taskAdjutant.createPacketBuffer(initialPayloadCapacity);
    }

    @Override
    public ByteBuf createPayloadBuffer(int initialPayloadCapacity) {
        return this.taskAdjutant.createPayloadBuffer(initialPayloadCapacity);
    }

    @Override
    public int obtainNegotiatedCapability() {
        Integer clientCapability = this.negotiatedCapability.get();
        if (clientCapability == null) {
            clientCapability = createNegotiatedCapability();
            this.negotiatedCapability.set(clientCapability);
        }
        return clientCapability;
    }

    @Override
    public ZoneOffset obtainZoneOffsetDatabase() {
        ZoneOffset zoneOffset = this.zoneOffsetDatabase.get();
        if (zoneOffset == null) {
            int phase = this.connectionPhase.get();
            if (phase > CONFIGURE_SESSION_PHASE) {
                throw new IllegalStateException("No yet config session");
            } else {
                throw new IllegalStateException("Config session error,not config zoneOffsetDatabase.");
            }
        }
        return zoneOffset;
    }

    @Override
    public ZoneOffset obtainZoneOffsetClient() {
        ZoneOffset zoneOffset = this.zoneOffsetClient.get();
        if (zoneOffset == null) {
            int phase = this.connectionPhase.get();
            if (phase > CONFIGURE_SESSION_PHASE) {
                throw new IllegalStateException("No yet config session");
            } else {
                throw new IllegalStateException("Config session error,not config zoneOffsetClient.");
            }
        }
        return zoneOffset;
    }

    @Override
    public int obtainMaxBytesPerCharClient() {
        return (int) obtainCharsetClient().newEncoder().maxBytesPerChar();
    }


    @Override
    public Charset obtainCharsetClient() {
        Charset charsetClient = this.charsetClient.get();
        if (charsetClient == null) {
            charsetClient = this.handshakeCharset;
        }
        return charsetClient;
    }


    @Override
    public Charset obtainCharsetResults() {
        Charset charsetClient = this.charsetResults.get();
        if (charsetClient == null) {
            charsetClient = StandardCharsets.UTF_8;
        }
        return charsetClient;
    }

    @Override
    public Map<Integer, CharsetMapping.CustomCollation> obtainCustomCollationMap() {
        Map<Integer, CharsetMapping.CustomCollation> map;
        map = this.customCollationMap.get();
        return (map != null && this.customCharsetNameSet.get() == null)// detect custom collation two phase finish.
                ? map
                : Collections.emptyMap();
    }

    @Override
    public HandshakeV10Packet obtainHandshakeV10Packet() {
        HandshakeV10Packet packet = this.handshakeV10Packet.get();
        if (packet == null) {
            throw new IllegalStateException("no handshake.");
        }
        return packet;
    }


    @Override
    public HostInfo obtainHostInfo() {
        return this.hostInfo;
    }


    /*################################## blow package method ##################################*/


    /**
     * <p>
     * must invoke firstly .
     * </p>
     * <p>
     * Receive HandshakeV10 packet send by MySQL server.
     * </p>
     *
     * @see #authenticateAndInitializing()
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html">Protocol::HandshakeV10</a>
     */
    Mono<MySQLPacket> receiveHandshake() {
        final int phase = this.connectionPhase.get();
        if (phase != HANDSHAKE_PHASE) {
            return createConnectionPhaseNotMatchException(phase);
        }
        return HandshakeV10Task.receive(this.taskAdjutant)
                .flatMap(this::handleHandshakeV10Packet)
                ;
    }

    /**
     * negotiate ssl for connection than hold by this instance.
     * <p>
     * must invoke after {@link #receiveHandshake()} and before {@link #authenticateAndInitializing()}
     * </p>
     *
     * @see #authenticateAndInitializing()
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase.html#sect_protocol_connection_phase_initial_handshake_ssl_handshake">Protocol::SSL Handshake</a>
     */
    Mono<PropertyDefinitions.SslMode> sslNegotiate() {
        final int phase = this.connectionPhase.get();
        if (phase != SSL_PHASE) {
            return createConnectionPhaseNotMatchException(phase);
        }
        if ((obtainNegotiatedCapability() & CLIENT_SSL) == 0) {
            return doAfterSendSslRequestEnd();
        }
        return SSLNegotiateTask.sslNegotiate(this.taskAdjutant)
                .then(Mono.defer(this::doAfterSendSslRequestEnd))
                ;
    }

    /**
     * <p>
     * must invoke after {@link #sslNegotiate()}.
     * </p>
     * <p>
     * do below:
     *     <ol>
     *     <li>send HandshakeResponse41 packet.</li>
     *     <li>handle more authentication exchange.</li>
     *     </ol>
     * </p>
     */
    Mono<Void> authenticate() {
        final int phase = this.connectionPhase.get();
        if (phase != AUTHENTICATION_PHASE) {
            return createConnectionPhaseNotMatchException(phase);
        }
        return AuthenticateTask.authenticate(this.taskAdjutant)
                .doOnSuccess(negotiatedCapability -> {
                    this.negotiatedCapability.set(negotiatedCapability);
                    this.connectionPhase.compareAndSet(AUTHENTICATION_PHASE, CONFIGURE_SESSION_PHASE);
                })
                .then()
                ;
    }

    /**
     * <p>
     * must invoke after {@link #authenticateAndInitializing()}
     * </p>
     * configure below url config session group properties:
     * <ul>
     *     <li>{@link PropertyKey#sessionVariables}</li>
     *     <li>{@link PropertyKey#characterEncoding}</li>
     *     <li>{@link PropertyKey#characterSetResults}</li>
     *     <li>{@link PropertyKey#connectionCollation}</li>
     * </ul>
     * <p>
     *     If {@link PropertyKey#detectCustomCollations} is true,then firstly handle this.
     * </p>
     */
    Mono<Void> configureSession() {
        final int phase = this.connectionPhase.get();
        if (phase != CONFIGURE_SESSION_PHASE) {
            return createConnectionPhaseNotMatchException(phase);
        }
        return detectCustomCollations()//1.
                .then(Mono.defer(this::executeSetSessionVariables))// 2.
                .then(Mono.defer(this::configureSessionCharset))//3.
                .then(Mono.defer(this::configZoneOffsets)) //4
                .then(Mono.defer(this::configureSessionSuccessEvent))//5.
                ;
    }

    /**
     * <p>
     * must invoke after {@link #configureSession()}
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
     * @see #authenticateAndInitializing()
     */
    Mono<Void> initialize() {
        final int phase = this.connectionPhase.get();
        if (phase != INITIALIZING_PHASE) {
            return createConnectionPhaseNotMatchException(phase);
        }
        final String autoCommitCommand = "SET autocommit = 0";
        final String isolationCommand = "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED";
        return commandUpdate(autoCommitCommand, EMPTY_STATE_CONSUMER)
                .doOnSuccess(rows -> LOG.debug("Command [{}] execute success.", autoCommitCommand))
                // blow 2 step
                .then(Mono.defer(() -> commandUpdate(isolationCommand, EMPTY_STATE_CONSUMER)))
                .doOnSuccess(rows -> LOG.debug("Command [{}]  execute success.", isolationCommand))
                .doOnSuccess(n -> this.connectionPhase.compareAndSet(INITIALIZING_PHASE, COMMAND_PHASE))
                .then()
                ;
    }



    /*################################## blow private method ##################################*/

    /**
     * @see #receiveHandshake()
     */
    private Mono<HandshakeV10Packet> handleHandshakeV10Packet(HandshakeV10Packet packet) {
        if (!this.handshakeV10Packet.compareAndSet(null, packet)) {
            return Mono.error(new JdbdMySQLException(
                    "%s can't concurrently invoke.handshakeV10Packet isn't null.", this));
        }

        return this.connectionPhase.compareAndSet(HANDSHAKE_PHASE, SSL_PHASE)
                ? Mono.just(packet)
                : createConcurrentlyConnectionException(HANDSHAKE_PHASE);
    }


    private Mono<PropertyDefinitions.SslMode> doAfterSendSslRequestEnd() {
        return this.connectionPhase.compareAndSet(SSL_PHASE, AUTHENTICATION_PHASE)
                ? Mono.justOrEmpty(this.properties.getProperty(PropertyKey.sslMode, PropertyDefinitions.SslMode.class))
                : createConcurrentlyConnectionException(SSL_PHASE);
    }

    /**
     * @see #configureSession()
     */
    private Mono<Void> detectCustomCollations() {
        if (!this.properties.getRequiredProperty(PropertyKey.detectCustomCollations, Boolean.class)) {
            return Mono.empty();
        }
        // blow tow phase: SHOW COLLATION phase and SHOW CHARACTER SET phase
        return commandQuery("SHOW COLLATION", ORIGINAL_ROW_DECODER, EMPTY_STATE_CONSUMER)
                .filter(this::isCustomCollation)
                .collectMap(this::customCollationMapKeyFunction, this::customCollationMapValueFunction)
                .doOnNext(this::createCustomCollationMapForShowCollation)
                // above SHOW COLLATION phase,blow SHOW CHARACTER SET phase
                .then(Mono.defer(this::detectCustomCharset));
    }

    /**
     * execute config session variables in {@link PropertyKey#sessionVariables}
     *
     * @see #configureSession()
     */
    private Mono<Void> executeSetSessionVariables() {
        String pairString = this.properties.getProperty(PropertyKey.sessionVariables);
        if (!MySQLStringUtils.hasText(pairString)) {
            return Mono.empty();
        }
        String command = ProtocolUtils.buildSetVariableCommand(pairString);
        if (LOG.isDebugEnabled()) {
            LOG.debug("execute set session variables:{}", command);
        }
        return commandUpdate(command, EMPTY_STATE_CONSUMER)
                .then()
                ;

    }


    /**
     * config three session variables:
     * <u>
     * <li>PropertyKey#characterEncoding</li>
     * <li>PropertyKey#characterSetResults</li>
     * <li>PropertyKey#connectionCollation</li>
     * </u>
     *
     * @see #configureSession()
     */
    private Mono<Void> configureSessionCharset() {

        CharsetMapping.Collation connectionCollation = tryObtainConnectionCollation();
        final Charset clientCharset;
        final String namesCommand;
        if (connectionCollation != null) {
            namesCommand = String.format("SET NAMES '%s' COLLATE '%s'"
                    , connectionCollation.mySQLCharset.charsetName
                    , connectionCollation.collationName);
            clientCharset = Charset.forName(connectionCollation.mySQLCharset.javaEncodingsUcList.get(0));
        } else {
            Pair<CharsetMapping.MySQLCharset, Charset> clientPair = obtainClientMySQLCharset();
            namesCommand = String.format("SET NAMES '%s'", clientPair.getFirst().charsetName);
            clientCharset = clientPair.getSecond();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("config session charset:{}", namesCommand);
        }
        // tow phase : SET NAMES and SET character_set_results = ?
        //below one phase SET NAMES
        return commandUpdate(namesCommand, EMPTY_STATE_CONSUMER)
                .doOnSuccess(rows -> commandSetNamesSuccessEvent(clientCharset))
                // below tow phase SET character_set_results = ?
                .then(Mono.defer(this::configCharsetResults))
                ;
    }

    /**
     * config {@link #zoneOffsetClient} and {@link #zoneOffsetDatabase}
     *
     * @see #configureSession()
     */
    private Mono<Void> configZoneOffsets() {
        //1. blow config zoneOffsetClient
        String zoneOffsetClientText = this.properties.getProperty(PropertyKey.zoneOffsetClient);
        ZoneOffset zoneOffsetClient;
        if (MySQLStringUtils.hasText(zoneOffsetClientText)) {
            try {
                zoneOffsetClient = ZoneOffset.of(zoneOffsetClientText);
            } catch (DateTimeException e) {
                return Mono.error(new JdbdMySQLException(e, "%s format error.", PropertyKey.zoneOffsetClient));
            }
        } else {
            zoneOffsetClient = MySQLTimeUtils.systemZoneOffset();
        }
        this.zoneOffsetClient.set(zoneOffsetClient);
        //2. blow config zoneOffsetDatabase
        return performanceZoneOffsetSession()
                .then(Mono.defer(this::retrieveAndConvertDatabaseTimeZone))
                // 3. blow check zoneOffsetClient and zoneOffsetDatabase
                .then(Mono.defer(this::checkAndAcceptZoneOffsets))
                ;
    }

    private Mono<Void> checkAndAcceptZoneOffsets() {
        ZoneOffset zoneOffsetClient, zoneOffsetDatabase;
        zoneOffsetClient = this.zoneOffsetClient.get();
        zoneOffsetDatabase = this.zoneOffsetDatabase.get();
        if (zoneOffsetClient == null || zoneOffsetDatabase == null) {
            return Mono.error(new JdbdMySQLException(
                    "Config ZoneOffset failure,zoneOffsetClient[%s],zoneOffsetDatabase[%s]"
                    , zoneOffsetClient, zoneOffsetDatabase));
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("zoneOffsetClient:[{}] ; zoneOffsetDatabase[{}]", zoneOffsetClient, zoneOffsetDatabase);
        }
        return Mono.empty();
    }

    /**
     * @see #configZoneOffsets() (back to invoker)
     */
    private Mono<Void> performanceZoneOffsetSession() {
        String zoneOffsetSessionText = this.properties.getProperty(PropertyKey.zoneOffsetSession);
        Mono<Void> mono;
        if (MySQLStringUtils.hasText(zoneOffsetSessionText)) {
            try {
                ZoneOffset zoneOffsetSession = ZoneOffset.of(zoneOffsetSessionText);
                String command = String.format("SET @@SESSION.time_zone = '%s'", zoneOffsetSession.getId());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("performant: {}", command);
                }
                mono = commandUpdate(command, EMPTY_STATE_CONSUMER)
                        .then()
                ;
            } catch (DateTimeException e) {
                mono = Mono.error(new JdbdMySQLException(e, "%s format error.", PropertyKey.zoneOffsetSession));
            }

        } else {
            mono = Mono.empty();
        }
        return mono;
    }

    /**
     * @see #configZoneOffsets()
     */
    private Mono<Void> retrieveAndConvertDatabaseTimeZone() {
        final long utcEpochSecond = OffsetDateTime.now(ZoneOffset.UTC).toEpochSecond();
        String command = String.format("SELECT @@SESSION.time_zone as timeZone,from_unixtime(%s) as databaseNow"
                , utcEpochSecond);
        return commandQuery(command, ORIGINAL_ROW_DECODER, EMPTY_STATE_CONSUMER)
                .elementAt(0)
                .flatMap(resultRow -> handleDatabaseTimeZone(resultRow, utcEpochSecond))
                ;
    }

    /**
     * @see #configZoneOffsets()
     */
    private Mono<Void> handleDatabaseTimeZone(ResultRow resultRow, final long utcEpochSecond) {
        String databaseZoneText = resultRow.getRequiredObject("timeZone", String.class);
        Mono<Void> mono;
        if ("SYSTEM".equalsIgnoreCase(databaseZoneText)) {
            LocalDateTime dateTime = resultRow.getRequiredObject("databaseNow", LocalDateTime.class);
            OffsetDateTime databaseNow = OffsetDateTime.of(dateTime, ZoneOffset.UTC);

            int totalSeconds = (int) (databaseNow.toEpochSecond() - utcEpochSecond);
            this.zoneOffsetDatabase.set(ZoneOffset.ofTotalSeconds(totalSeconds));
            mono = Mono.empty();
        } else {
            try {
                ZoneId databaseZone = ZoneId.of(databaseZoneText, ZoneId.SHORT_IDS);
                this.zoneOffsetDatabase.set(MySQLTimeUtils.toZoneOffset(databaseZone));
                mono = Mono.empty();
            } catch (DateTimeException e) {
                mono = Mono.error(new JdbdMySQLException(e, "MySQL time_zone[%s] cannot convert to %s ."
                        , databaseZoneText, ZoneId.class.getName()));
            }
        }
        return mono;
    }


    /**
     * @see #configureSessionCharset()
     * @see #commandSetNamesSuccessEvent(Charset)
     */
    private Mono<Void> configCharsetResults() {
        final String charset = this.properties.getProperty(PropertyKey.characterSetResults, String.class);
        if (charset != null && !MySQLStringUtils.hasText(charset)) {
            return Mono.empty();
        }
        final Charset charsetResults;
        final String command;
        if (charset == null || charset.equalsIgnoreCase("NULL")) {
            command = "SET character_set_results = NULL";
            charsetResults = null;
        } else {
            ServerVersion serverVersion = obtainHandshakeV10Packet().getServerVersion();
            CharsetMapping.MySQLCharset mySQLCharset;
            mySQLCharset = CharsetMapping.getMysqlCharsetForJavaEncoding(charset, serverVersion);
            if (mySQLCharset == null) {
                return Mono.error(new JdbdMySQLException(
                        "No found MySQL charset fro Property characterSetResults[%s]", charset));
            }
            command = String.format("SET character_set_results = '%s'", mySQLCharset.charsetName);
            charsetResults = Charset.forName(charset);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("config charset result:{}", command);
        }
        return commandUpdate(command, EMPTY_STATE_CONSUMER)
                .flatMap(rows -> overrideCharsetResults(charsetResults));

    }

    /**
     * override {@link #charsetResults}
     *
     * @see #configCharsetResults()
     */
    private Mono<Void> overrideCharsetResults(@Nullable Charset charsetResults) {
        if (charsetResults != null) {
            this.charsetResults.set(charsetResults);
            return Mono.empty();
        }
        String command = "SHOW VARIABLES WHERE Variable_name = 'character_set_system'";
        if (LOG.isDebugEnabled()) {
            LOG.debug("override charsetResults,execute command: {}", command);
        }
        return commandQuery(command, ORIGINAL_ROW_DECODER, EMPTY_STATE_CONSUMER)
                .elementAt(0)
                .doOnNext(this::updateCharsetResultsAfterQueryCharacterSetSystem)
                .then()
                ;
    }

    /**
     * update {@link #charsetResults}
     *
     * @see #overrideCharsetResults(Charset)
     */
    private void updateCharsetResultsAfterQueryCharacterSetSystem(ResultRow resultRow) {
        final String charset = resultRow.getObject("Value", String.class);
        if (charset == null || charset.equalsIgnoreCase("NULL")) {
            this.charsetResults.set(StandardCharsets.UTF_8);
        } else {
            ServerVersion serverVersion = obtainHandshakeV10Packet().getServerVersion();
            CharsetMapping.MySQLCharset mySQLCharset;
            mySQLCharset = CharsetMapping.getMysqlCharsetForJavaEncoding(charset, serverVersion);
            if (mySQLCharset == null) {
                throw new JdbdMySQLException(
                        "Not found java charset for character_set_system[%s]", charset);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("override charsetResults with character_set_system:{}", charset);
            }
            this.charsetResults.set(Charset.forName(mySQLCharset.javaEncodingsUcList.get(0)));
        }
    }


    /**
     * @see #configureSessionCharset()
     * @see #configCharsetResults()
     */
    private void commandSetNamesSuccessEvent(Charset clientCharset) {
        this.charsetClient.set(clientCharset);
        this.charsetResults.set(clientCharset);
    }


    @Nullable
    private CharsetMapping.Collation tryObtainConnectionCollation() {
        CharsetMapping.Collation collationConnection;
        String connectionCollation = this.properties.getProperty(PropertyKey.connectionCollation, String.class);
        if (!MySQLStringUtils.hasText(connectionCollation)) {
            return null;
        }
        collationConnection = CharsetMapping.getCollationByName(connectionCollation);
        if (collationConnection != null
                && CharsetMapping.isUnsupportedCharset(collationConnection.mySQLCharset.charsetName)) {
            collationConnection = CharsetMapping.INDEX_TO_COLLATION.get(CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4);
        }
        return collationConnection;
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
                || !CharsetMapping.isUnsupportedCharset(charsetClient)
                || StandardCharsets.UTF_8.name().equalsIgnoreCase(charsetClient)
                || StandardCharsets.UTF_8.aliases().contains(charsetClient)) {
            pair = defaultPair;
        } else {
            ServerVersion serverVersion = obtainHandshakeV10Packet().getServerVersion();
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


    /**
     * handle {@link #configureSession()} success event.
     *
     * @see #configureSession()
     */
    private Mono<Void> configureSessionSuccessEvent() {
        if (!this.connectionPhase.compareAndSet(CONFIGURE_SESSION_PHASE, INITIALIZING_PHASE)) {
            return createConcurrentlyConnectionException(CONFIGURE_SESSION_PHASE);
        }
        Charset charsetClient = this.charsetClient.get();
        Charset charsetResults = this.charsetResults.get();
        if (charsetClient == null || charsetResults == null) {
            return Mono.error
                    (new JdbdMySQLException("charsetClient:{},charsetResults:{}", charsetClient, charsetResults));
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("charsetClient:{},charsetResults:{}", charsetClient, charsetResults);
        }
        return Mono.empty();
    }

    private <T> Mono<T> createConcurrentlyConnectionException(int expected) {
        return Mono.error(new JdbdMySQLException(
                "%s can't concurrently invoke.connectionPhase expected[%s] but not.", this, expected));
    }

    private <T> Mono<T> createConnectionPhaseNotMatchException(int currentPhase) {
        return Mono.error(new JdbdMySQLException("Connection phase not match,current phase[%s]", currentPhase));
    }


    /**
     * @see #detectCustomCollations()
     */
    private void createCustomCollationMapForShowCollation(Map<Integer, CharsetMapping.CustomCollation> map) {
        if (map.isEmpty()) {
            this.customCollationMap.set(Collections.emptyMap());
            this.customCharsetNameSet.set(Collections.emptySet());
        } else {

            Set<String> charsetNameSet = new HashSet<>();
            for (CharsetMapping.CustomCollation collation : map.values()) {
                if (!CharsetMapping.CHARSET_NAME_TO_CHARSET.containsKey(collation.charsetName)) {
                    throw new JdbdMySQLException("Custom collation[%s] not found corresponding java charset."
                            , collation.charsetName);
                }
                charsetNameSet.add(collation.charsetName);
            }
            //firstly customCharsetNameSet
            this.customCharsetNameSet.set(Collections.unmodifiableSet(charsetNameSet));
            // secondly customCollationMap
            this.customCollationMap.set(Collections.unmodifiableMap(map));
        }
    }

    private Integer customCollationMapKeyFunction(ResultRow resultRow) {
        return resultRow.getRequiredObject("Id", Integer.class);
    }

    private CharsetMapping.CustomCollation customCollationMapValueFunction(ResultRow resultRow) {
        return new CharsetMapping.CustomCollation(
                resultRow.getRequiredObject("Id", Integer.class)
                , resultRow.getRequiredObject("Collation", String.class)
                , resultRow.getRequiredObject("Charset", String.class)
                , -1 // placeholder.
        );
    }

    /**
     * @see #detectCustomCollations()
     */
    private boolean isCustomCollation(ResultRow resultRow) {
        return !CharsetMapping.INDEX_TO_COLLATION.containsKey(resultRow.getRequiredObject("Id", Integer.class));
    }

    /**
     * @see #detectCustomCollations()
     */
    private Mono<Void> detectCustomCharset() {
        final Set<String> charsetNameSet = this.customCharsetNameSet.get();
        Mono<Void> mono;
        if (charsetNameSet == null) {
            mono = Mono.error(new JdbdMySQLException("no detect custom collation."));
        } else if (charsetNameSet.isEmpty()) {
            mono = Mono.empty();
        } else {

            mono = commandQuery("SHOW CHARACTER SET", ORIGINAL_ROW_DECODER, EMPTY_STATE_CONSUMER)
                    .filter(resultRow -> charsetNameSet.contains(resultRow.getRequiredObject("Charset", String.class)))
                    .collectMap(resultRow -> resultRow.getRequiredObject("Charset", String.class)
                            , resultRow -> resultRow.getRequiredObject("Maxlen", Integer.class))
                    .doOnNext(this::createCustomCollations)
                    .then();
        }
        return mono;
    }


    /**
     * @see #detectCustomCharset()
     */
    private void createCustomCollations(final Map<String, Integer> customCharsetToMaxLenMap) {
        // oldCollationMap no max length of charset.
        Map<Integer, CharsetMapping.CustomCollation> oldCollationMap = this.customCollationMap.get();
        if (oldCollationMap == null) {
            throw new IllegalStateException("No detect custom collation.");
        }
        Map<Integer, CharsetMapping.CustomCollation> newCollationMap = new HashMap<>();
        for (Map.Entry<Integer, CharsetMapping.CustomCollation> e : oldCollationMap.entrySet()) {
            Integer index = e.getKey();
            CharsetMapping.CustomCollation collation = e.getValue();

            String charsetName = collation.charsetName;
            Integer maxLen = customCharsetToMaxLenMap.get(charsetName);
            CharsetMapping.CustomCollation newCollation = new CharsetMapping.CustomCollation(
                    index, collation.collationName, charsetName, maxLen);

            newCollationMap.put(index, newCollation);
        }
        //firstly customCollationMap
        this.customCollationMap.set(Collections.unmodifiableMap(newCollationMap));
        //secondly customCharsetNameSet
        this.customCharsetNameSet.set(null);
    }


    private int createNegotiatedCapability() {
        HandshakeV10Packet handshakeV10Packet = obtainHandshakeV10Packet();
        final int serverCapability = handshakeV10Packet.getCapabilityFlags();
        final Properties env = this.properties;

        final boolean useConnectWithDb = MySQLStringUtils.hasText(this.hostInfo.getDatabase())
                && !env.getRequiredProperty(PropertyKey.createDatabaseIfNotExist, Boolean.class);

        return ClientProtocol.CLIENT_SECURE_CONNECTION
                | ClientProtocol.CLIENT_PLUGIN_AUTH
                | (serverCapability & ClientProtocol.CLIENT_LONG_PASSWORD)  //
                | (serverCapability & ClientProtocol.CLIENT_PROTOCOL_41)    //

                | (serverCapability & ClientProtocol.CLIENT_TRANSACTIONS)   // Need this to get server status values
                | (serverCapability & ClientProtocol.CLIENT_MULTI_RESULTS)  // We always allow multiple result sets
                | (serverCapability & ClientProtocol.CLIENT_PS_MULTI_RESULTS)  // We always allow multiple result sets for SSPS
                | (serverCapability & ClientProtocol.CLIENT_LONG_FLAG)      //

                | (serverCapability & ClientProtocol.CLIENT_DEPRECATE_EOF)  //
                | (serverCapability & ClientProtocol.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
                | (env.getRequiredProperty(PropertyKey.useCompression, Boolean.class) ? (serverCapability & ClientProtocol.CLIENT_COMPRESS) : 0)
                | (useConnectWithDb ? (serverCapability & ClientProtocol.CLIENT_CONNECT_WITH_DB) : 0)
                | (env.getRequiredProperty(PropertyKey.useAffectedRows, Boolean.class) ? 0 : (serverCapability & ClientProtocol.CLIENT_FOUND_ROWS))

                | (env.getRequiredProperty(PropertyKey.allowLoadLocalInfile, Boolean.class) ? (serverCapability & ClientProtocol.CLIENT_LOCAL_FILES) : 0)
                | (env.getRequiredProperty(PropertyKey.interactiveClient, Boolean.class) ? (serverCapability & ClientProtocol.CLIENT_INTERACTIVE) : 0)
                | (env.getRequiredProperty(PropertyKey.allowMultiQueries, Boolean.class) ? (serverCapability & ClientProtocol.CLIENT_MULTI_STATEMENTS) : 0)
                | (env.getRequiredProperty(PropertyKey.disconnectOnExpiredPasswords, Boolean.class) ? 0 : (serverCapability & ClientProtocol.CLIENT_CAN_HANDLE_EXPIRED_PASSWORD))

                | (NONE.equals(env.getProperty(PropertyKey.connectionAttributes)) ? 0 : (serverCapability & ClientProtocol.CLIENT_CONNECT_ATTRS))
                | (env.getRequiredProperty(PropertyKey.sslMode, PropertyDefinitions.SslMode.class) != PropertyDefinitions.SslMode.DISABLED ? (serverCapability & ClientProtocol.CLIENT_SSL) : 0)

                // TODO MYSQLCONNJ-437
                // clientParam |= (capabilityFlags & NativeServerSession.CLIENT_SESSION_TRACK);

                ;
    }



    /*################################## blow static method ##################################*/


}
