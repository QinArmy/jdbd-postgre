package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.MultiResults;
import io.jdbd.ResultRow;
import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.session.MySQLSessionAdjutant;
import io.jdbd.vendor.conf.HostInfo;
import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.util.SQLStates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

final class ClientConnectionProtocolImpl implements ClientConnectionProtocol {

    private static final Logger LOG = LoggerFactory.getLogger(ClientConnectionProtocolImpl.class);


    static Mono<ClientConnectionProtocolImpl> create(final int hostIndex, MySQLSessionAdjutant sessionAdjutant) {
        return MySQLTaskExecutor.create(hostIndex, sessionAdjutant)
                .flatMap(taskExecutor -> {
                    ClientConnectionProtocolImpl protocol;
                    protocol = new ClientConnectionProtocolImpl(sessionAdjutant, hostIndex, taskExecutor);
                    return protocol.authenticateAndInitializing()
                            .thenReturn(protocol);
                })

                ;
    }


    final HostInfo<PropertyKey> hostInfo;

    final MySQLTaskExecutor taskExecutor;

    private final Properties<PropertyKey> properties;

    private final AtomicReference<Map<Integer, CharsetMapping.CustomCollation>> customCollationMap = new AtomicReference<>(null);

    private final AtomicReference<Set<String>> customCharsetNameSet = new AtomicReference<>(null);

    final SessionResetter sessionResetter;


    private ClientConnectionProtocolImpl(final MySQLSessionAdjutant sessionAdjutant
            , int hostIndex, final MySQLTaskExecutor taskExecutor) {
        this.hostInfo = sessionAdjutant.obtainUrl().getHostList().get(hostIndex);
        this.taskExecutor = taskExecutor;
        this.sessionResetter = DefaultSessionResetter.create(this.taskExecutor.getAdjutant());
        this.properties = this.hostInfo.getProperties();
    }


    @Override
    public Mono<Void> authenticateAndInitializing() {
        return MySQLConnectionTask.authenticate(this.taskExecutor.getAdjutant())
                .doOnSuccess(result -> MySQLTaskExecutor.setAuthenticateResult(this.taskExecutor, result))
                .then(Mono.defer(this::detectCustomCollations))
                .doOnSuccess(map -> MySQLTaskExecutor.setCustomCollation(this.taskExecutor, map))

                .then(Mono.defer(this.sessionResetter::reset)) // reset session.
                .doOnSuccess(server -> MySQLTaskExecutor.resetTaskAdjutant(this.taskExecutor, server))
                .then()
                .onErrorResume(this::handleInitializingError)
                ;
    }

    @Override
    public Mono<Void> closeGracefully() {
        return QuitTask.quit(this.taskExecutor.getAdjutant());
    }


    /*################################## blow package method ##################################*/

    /**
     * @see #authenticateAndInitializing()
     */
    private Mono<Void> handleInitializingError(Throwable e) {
        Mono<Void> mono;
        if (this.taskExecutor.getAdjutant().isAuthenticated()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("initializing occur error,quit session.");
            }
            mono = closeGracefully()
                    .then(Mono.error(e));
        } else {
            mono = Mono.error(e);
        }
        return mono;
    }

    /**
     * @see #authenticateAndInitializing()
     */
    private Mono<Map<Integer, CharsetMapping.CustomCollation>> detectCustomCollations() {

        Mono<Map<Integer, CharsetMapping.CustomCollation>> mono;
        if (this.properties.getOrDefault(PropertyKey.detectCustomCollations, Boolean.class)) {
            LOG.debug("detectCustomCollations start");
            // blow tow phase: SHOW COLLATION phase and SHOW CHARACTER SET phase
            mono = ComQueryTask.query("SHOW COLLATION", MultiResults.EMPTY_CONSUMER, this.taskExecutor.getAdjutant())
                    .filter(this::isCustomCollation)
                    .doOnNext(this::printCustomCollationLog)
                    .collectMap(this::customCollationMapKeyFunction, this::customCollationMapValueFunction)
                    .flatMap(this::createCustomCollationMapForShowCollation)
                    // above SHOW COLLATION phase,blow SHOW CHARACTER SET phase
                    .then(Mono.defer(this::detectCustomCharset));
        } else {
            LOG.debug("no detectCustomCollations,return empty map.");
            mono = Mono.just(Collections.emptyMap());
        }

        return mono;
    }



    /*################################## blow private method ##################################*/

    private void printCustomCollationLog(ResultRow row) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("has custom collations : {} - {} "
                    , row.obtain("Id"), row.obtain("Collation"));
        }
    }


    /**
     * @see #detectCustomCollations()
     */
    private Mono<Void> createCustomCollationMapForShowCollation(Map<Integer, CharsetMapping.CustomCollation> map) {
        LOG.debug("createCustomCollationMapForShowCollation start");
        if (map.isEmpty()) {
            this.customCollationMap.set(Collections.emptyMap());
            this.customCharsetNameSet.set(Collections.emptySet());
        } else {

            Set<String> charsetNameSet = new HashSet<>();
            for (CharsetMapping.CustomCollation collation : map.values()) {
                if (!CharsetMapping.CHARSET_NAME_TO_CHARSET.containsKey(collation.charsetName)) {
                    String message = String.format("Custom collation[%s] not found corresponding java charset."
                            , collation.charsetName);
                    return Mono.error(new JdbdSQLException(new SQLException(message, SQLStates.CONNECTION_EXCEPTION)));
                }
                charsetNameSet.add(collation.charsetName);
            }
            //firstly customCharsetNameSet
            this.customCharsetNameSet.set(Collections.unmodifiableSet(charsetNameSet));
            // secondly customCollationMap
            this.customCollationMap.set(Collections.unmodifiableMap(map));
        }
        return Mono.empty();
    }

    /**
     * @see #detectCustomCollations()
     */
    private Integer customCollationMapKeyFunction(ResultRow resultRow) {
        return resultRow.obtain("Id", Integer.class);
    }

    /**
     * @see #detectCustomCollations()
     */
    private CharsetMapping.CustomCollation customCollationMapValueFunction(ResultRow resultRow) {
        return new CharsetMapping.CustomCollation(
                resultRow.obtain("Id", Integer.class)
                , resultRow.obtain("Collation", String.class)
                , resultRow.obtain("Charset", String.class)
                , -1 // placeholder.
        );
    }

    /**
     * @see #detectCustomCollations()
     */
    private boolean isCustomCollation(ResultRow resultRow) {
        return !CharsetMapping.INDEX_TO_COLLATION.containsKey(resultRow.obtain("Id", Integer.class));
    }

    /**
     * @see #detectCustomCollations()
     */
    private Mono<Map<Integer, CharsetMapping.CustomCollation>> detectCustomCharset() {
        LOG.debug("detectCustomCharset start");
        final Set<String> charsetNameSet = this.customCharsetNameSet.get();
        Mono<Map<Integer, CharsetMapping.CustomCollation>> mono;
        if (charsetNameSet == null) {
            mono = Mono.error(new JdbdSQLException(new SQLException("no detect custom collation."
                    , SQLStates.CONNECTION_EXCEPTION)));
        } else if (charsetNameSet.isEmpty()) {
            mono = Mono.just(Collections.emptyMap());
        } else {

            mono = ComQueryTask.query("SHOW CHARACTER SET", MultiResults.EMPTY_CONSUMER, this.taskExecutor.getAdjutant())
                    .filter(resultRow -> charsetNameSet.contains(resultRow.obtain("Charset", String.class)))
                    .collectMap(resultRow -> resultRow.obtain("Charset", String.class)
                            , resultRow -> resultRow.obtain("Maxlen", Integer.class))
                    .map(this::createCustomCollations);
        }
        return mono;
    }


    /**
     * @see #detectCustomCharset()
     */
    private Map<Integer, CharsetMapping.CustomCollation> createCustomCollations(
            final Map<String, Integer> customCharsetToMaxLenMap) {
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
        this.customCollationMap.set(null);
        //secondly customCharsetNameSet
        this.customCharsetNameSet.set(null);
        return Collections.unmodifiableMap(newCollationMap);
    }


}
