package io.jdbd.mysql.protocol.client;

import io.jdbd.ReactiveSQLException;
import io.jdbd.ResultRow;
import io.jdbd.ResultRowMeta;
import io.jdbd.ResultStates;
import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.EofPacket;
import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.protocol.OkPacket;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.netty.Connection;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;

abstract class AbstractClientProtocol implements ClientProtocol, ResultRowAdjutant {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractClientProtocol.class);



    final Connection connection;

    final MySQLUrl mySQLUrl;

    final MySQLCumulateReceiver cumulateReceiver;

    final Properties properties;

    AbstractClientProtocol(Connection connection, MySQLUrl mySQLUrl, MySQLCumulateReceiver cumulateReceiver) {
        this.connection = connection;
        this.mySQLUrl = mySQLUrl;
        this.cumulateReceiver = cumulateReceiver;

        this.properties = mySQLUrl.getHosts().get(0).getProperties();
    }

    @Override
    public MultiResults command(String command) {
        return null;
    }


    @Override
    public final Mono<Long> commandUpdate(String command, Consumer<ResultStates> statesConsumer) {
        return sendCommandPacket(command)
                .then(Mono.defer(() -> receiveCommandUpdatePacket(statesConsumer)));
    }


    @Override
    public final <T> Flux<T> commandQuery(String command, BiFunction<ResultRow, ResultRowMeta, T> rowDecoder
            , Consumer<ResultStates> statesConsumer) {
        return sendCommandPacket(command) //1. send COM_QUERY packet
                .then(Mono.defer(this::receiveResultSetMetadataPacket))//2. receive row meta packet
                // 3. receive rows and decode with rowDecoder
                .flatMapMany(rowMeta -> receiveResultSetRows(rowMeta, rowDecoder, statesConsumer))
                ;
    }

    @Override
    public final Mono<Void> closeGracefully() {
        // TODO optimize
        ByteBuf packetBuf = createPacketBuffer(1);
        packetBuf.writeByte(PacketUtils.COM_QUIT_HEADER);
        return sendPacket(packetBuf, null)
                .then();
    }

    public final ByteBuf createPacketBuffer(int initialPayloadCapacity) {
        return PacketUtils.createPacketBuffer(this.connection, initialPayloadCapacity);
    }



    /*################################## blow ResultRowAdjutant method ##################################*/


    final BiFunction<ByteBuf, MySQLColumnMeta, Object> obtainResultColumnParser(MySQLType mySQLType) {
        BiFunction<ByteBuf, MySQLColumnMeta, Object> function = this.resultColumnParserMap.get(mySQLType);
        if (function == null) {
            throw new JdbdMySQLException("Not found column parser for %s", mySQLType);
        }
        return function;
    }

    /**
     * @see #commandQuery(String, BiFunction, Consumer)
     * @see #commandUpdate(String, Consumer)
     */
    final Mono<Void> sendPacket(ByteBuf packetBuffer, @Nullable AtomicInteger sequenceId) {
        // TODO optimize packet send
        PacketUtils.writePacketHeader(packetBuffer, sequenceId == null ? 0 : sequenceId.addAndGet(1));
        return Mono.from(this.connection.outbound()
                .send(Mono.just(packetBuffer)));
    }


    abstract ZoneOffset obtainDatabaseZoneOffset();

    abstract int obtainNegotiatedCapability();

    abstract Charset obtainCharsetClient();

    /**
     * be equivalent to {@code (int)obtainCharsetClient().newEncoder().maxBytesPerChar()}
     */
    abstract int obtainMaxBytesPerCharClient();

    abstract Charset obtainCharsetResults();

    /**
     * @return a unmodifiable map
     */
    abstract Map<Integer, CharsetMapping.CustomCollation> obtainCustomCollationMap();

    /*################################## blow private method ##################################*/




    /**





    /**
     * @see #receiveResultSetRows(MySQLRowMeta, BiFunction, Consumer)
     */
    private boolean textResultSetMultiRowDecoder(final ByteBuf cumulateBuf, FluxSink<ByteBuf> sink) {
        return PacketDecoders.textResultSetMultiRowDecoder(cumulateBuf, sink, obtainNegotiatedCapability());
    }

    /**
     * @see #commandQuery(String, BiFunction, Consumer)
     */
    private Mono<MySQLRowMeta> receiveResultSetMetadataPacket() {
        //  comQueryResultSetMetaDecoder will handle not expected packet,eg:ok,LOCAL INFILE Request .
        return this.cumulateReceiver.receiveOne(this::textResultSetMetaDecoder)
                .map(this::parseResultSetMetaPacket)
                ;
    }

    /**
     * @see #receiveResultSetMetadataPacket()
     */
    private boolean textResultSetMetaDecoder(final ByteBuf cumulateBuf, final MonoSink<ByteBuf> sink) {
        if (!PacketUtils.hasOnePacket(cumulateBuf)) {
            return false;
        }
        final int negotiatedCapability = obtainNegotiatedCapability();
        final CommandStatementTask.ComQueryResponse responseType;
        responseType = CommandStatementTask.detectComQueryResponseType(cumulateBuf, negotiatedCapability);
        boolean decodeEnd;
        switch (responseType) {
            case OK:
            case LOCAL_INFILE_REQUEST:
                // expected TEXT_RESULT, but OK/LOCAL_INFILE_REQUEST, read packet and drop.
                decodeEnd = PacketDecoders.packetDecoder(cumulateBuf, PacketDecoders.SEWAGE_MONO_SINK);//SEWAGE_SINK for drop
                if (decodeEnd) {
                    // notify downstream command sql not match
                    sink.error(MySQLExceptionUtils.createNonResultSetCommandException());
                }
                break;
            case ERROR:
                ErrorPacket packet;
                packet = PacketDecoders.decodeErrorPacket(cumulateBuf, negotiatedCapability, obtainCharsetResults());
                // notify downstream command sql error.
                sink.error(MySQLExceptionUtils.createErrorPacketException(packet));
                decodeEnd = true;
                break;
            case TEXT_RESULT:
                decodeEnd = PacketDecoders.textResultSetMetadataDecoder(cumulateBuf, sink, negotiatedCapability);
                break;
            default:
                throw MySQLExceptionUtils.createUnknownEnumException(responseType);
        }
        return decodeEnd;
    }

    /**
     * @see #receiveResultSetMetadataPacket()
     */
    private MySQLRowMeta parseResultSetMetaPacket(ByteBuf rowMetaOrErrorPacket) {
        CommandStatementTask.ComQueryResponse response;
        final int negotiatedCapability = obtainNegotiatedCapability();
        response = CommandStatementTask.detectComQueryResponseType(rowMetaOrErrorPacket, negotiatedCapability);

        if (response == CommandStatementTask.ComQueryResponse.TEXT_RESULT) {

            MySQLColumnMeta[] columnMetas = CommandStatementTask.readResultColumnMetas(
                    rowMetaOrErrorPacket, negotiatedCapability
                    , obtainCharsetResults(), this.properties);
            return MySQLRowMeta.from(columnMetas, obtainCustomCollationMap());
        }
        throw new IllegalArgumentException("rowMetaOrErrorPacket error");

    }

    /**
     * @see #commandUpdate(String, Consumer)
     */
    private Mono<Long> receiveCommandUpdatePacket(Consumer<ResultStates> statesConsumer) {
        return this.cumulateReceiver.receiveOnePacket()
                .flatMap(packetBuf -> commandUpdateResultHandler(packetBuf, statesConsumer))
                ;
    }

    private enum ErrorType {
        TEXT_RESULT,
        LOCAL_INFILE_REQUEST
    }

    private abstract class AbstractCommandUpdateResultDecoder implements BiFunction<ByteBuf, MonoSink<ByteBuf>, Boolean> {

        //non-volatile ,because update in Netty EvenLoop .
        int textResultPhase = 0;

        private ErrorType errorType;

        @Override
        public final Boolean apply(ByteBuf cumulateBuf, MonoSink<ByteBuf> sink) {
            boolean decodeEnd = false;
            if (this.textResultPhase == 0) {
                // no error, command and Statement method match.
                decodeEnd = decodeResult(cumulateBuf, sink);
            }
            if (decodeEnd && this.textResultPhase == 0) {

            }
            // command and Statement method not match.
            switch (this.textResultPhase) {
                case 1:
                    if (!textResultSetMetaDecoder(cumulateBuf, PacketDecoders.SEWAGE_MONO_SINK)) {
                        break;
                    }
                    this.textResultPhase++;
                    if (!textResultSetMultiRowDecoder(cumulateBuf, PacketDecoders.SEWAGE_FLUX_SINK)) {
                        break;
                    }
                    this.textResultPhase++;
                    if (!PacketDecoders.packetDecoder(cumulateBuf, PacketDecoders.SEWAGE_MONO_SINK)) {
                        break;
                    }
                    this.textResultPhase++;
                    break;
                case 2:
                    if (!textResultSetMultiRowDecoder(cumulateBuf, PacketDecoders.SEWAGE_FLUX_SINK)) {
                        break;
                    }
                    this.textResultPhase++;
                    if (!PacketDecoders.packetDecoder(cumulateBuf, PacketDecoders.SEWAGE_MONO_SINK)) {
                        break;
                    }
                    this.textResultPhase++;
                    break;
                case 3:
                    if (!PacketDecoders.packetDecoder(cumulateBuf, PacketDecoders.SEWAGE_MONO_SINK)) {
                        break;
                    }
                    this.textResultPhase++;
                    break;
                default:
                    throw new IllegalStateException(
                            String.format("textResultPhase[%s] state error.", this.textResultPhase));
            }
            return this.textResultPhase > 3;
        }


        final void commandError(ErrorType errorType) {
            this.errorType = errorType;
        }

        abstract boolean decodeResult(ByteBuf byteBuf, MonoSink<ByteBuf> sink);

    }

    private final class CommandUpdateResultDecoder extends AbstractCommandUpdateResultDecoder {
        @Override
        boolean decodeResult(ByteBuf cumulateBuf, MonoSink<ByteBuf> sink) {
            if (!PacketUtils.hasOnePacket(cumulateBuf)) {
                return false;
            }
            final int negotiatedCapability = obtainNegotiatedCapability();
            final CommandStatementTask.ComQueryResponse type;
            type = CommandStatementTask.detectComQueryResponseType(cumulateBuf, negotiatedCapability);
            boolean decodeEnd;
            switch (type) {
                case ERROR:
                    ErrorPacket error;
                    error = PacketDecoders.decodeErrorPacket(cumulateBuf, negotiatedCapability, obtainCharsetResults());
                    sink.error(MySQLExceptionUtils.createErrorPacketException(error));
                    decodeEnd = true;
                    break;
                case OK:
                    decodeEnd = PacketDecoders.packetDecoder(cumulateBuf, sink);
                    break;
                case TEXT_RESULT:
                    this.commandError(ErrorType.TEXT_RESULT);
                    decodeEnd = false;
                    break;
                case LOCAL_INFILE_REQUEST:
                    this.commandError(ErrorType.LOCAL_INFILE_REQUEST);
                    decodeEnd = false;
                    break;
                default:
                    throw MySQLExceptionUtils.createUnknownEnumException(type);
            }
            return decodeEnd;
        }
    }

    private final class CommandLocalInfileRequestDecoder extends AbstractCommandUpdateResultDecoder {
        @Override
        boolean decodeResult(ByteBuf byteBuf, MonoSink<ByteBuf> sink) {
            return false;
        }
    }


    /**
     * @see #receiveCommandUpdatePacket(Consumer)
     */
    private Mono<Long> commandUpdateResultHandler(ByteBuf packetBuf, Consumer<ResultStates> statesConsumer) {
        packetBuf.skipBytes(3);
        if (packetBuf.readByte() != 1) {
            return Mono.error(new ReactiveSQLException(
                    new SQLException("Read COM_QUERY response error,sequence_id isn't 1.")));
        }

        if (ErrorPacket.isErrorPacket(packetBuf)) {
            ErrorPacket error = ErrorPacket.readPacket(packetBuf, obtainNegotiatedCapability(), obtainCharsetResults());
            return Mono.error(MySQLExceptionUtils.createErrorPacketException(error));
        }

        OkPacket okPacket = OkPacket.readPacket(packetBuf, obtainNegotiatedCapability());
        final ResultStates resultStates = MySQLResultStates.from(okPacket);
        return Mono.just(okPacket.getAffectedRows())
                .concatWith(Flux.defer(() -> commandUpdateStateConsume(resultStates, statesConsumer)))
                .elementAt(0)
                ;
    }

    /**
     * @return {@link Flux#empty()} or {@link Flux#error(Throwable)}
     */
    private Flux<Long> commandUpdateStateConsume(ResultStates resultStates, Consumer<ResultStates> statesConsumer) {
        Flux<Long> flux;
        try {
            statesConsumer.accept(resultStates);
            flux = Flux.empty();
        } catch (Throwable e) {
            flux = Flux.error(MySQLExceptionUtils.wrapExceptionIfNeed(e));
        }
        return flux;
    }


    /**
     * @see #commandQuery(String, BiFunction, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html">Protocol::Text Resultset</a>
     */
    private <T> Flux<T> receiveResultSetRows(MySQLRowMeta rowMeta
            , BiFunction<ResultRow, ResultRowMeta, T> rowDecoder, Consumer<ResultStates> statesConsumer) {
        return this.cumulateReceiver.receive(this::textResultSetMultiRowDecoder) // 1. decode packet,if error skip packet and publish error.
                .flatMap(multiRowBuf -> {
                    multiRowBuf.retain(); // for below convertResultRow method.
                    return Flux.<ResultRow>create(sink -> parseResultSetRows(sink, multiRowBuf, rowMeta));
                }) //2.parse ByteBuf to ResultRow
                .map(resultRow -> decodeResultRow(resultRow, rowMeta, rowDecoder)) //3. convert ResultRow to T
                .onErrorResume(this::handleResultSetTerminatorOnError) // optionally handle upstream error and terminator,finally publish error.
                .doOnCancel(() -> handleResultSetTerminator(statesConsumer).subscribe(v -> {
                }))
                .concatWith(Flux.defer(() -> handleResultSetTerminator(statesConsumer))) // 4.handle result set terminator,don't publish ResultRow
                ;
    }


    /**
     * @see #receiveResultSetRows(MySQLRowMeta, BiFunction, Consumer)
     */
    private <T> T decodeResultRow(ResultRow resultRow, MySQLRowMeta rowMeta
            , BiFunction<ResultRow, ResultRowMeta, T> rowDecoder) {
        T finalRow;
        try {
            finalRow = rowDecoder.apply(resultRow, rowMeta);
        } catch (Throwable e) {
            throw new ReactiveSQLException(new SQLException("rowDecoder throw exception.", e));
        }
        if (finalRow == null) {
            throw new ReactiveSQLException(new SQLException("Result Set decoder can't return null."));
        }
        return finalRow;
    }


    /**
     * @return {@link Flux#error(Throwable)}
     * @see #receiveResultSetRows(MySQLRowMeta, BiFunction, Consumer)
     */
    private <T> Flux<T> handleResultSetTerminatorOnError(Throwable e) {
        return handleResultSetTerminator(EMPTY_STATE_CONSUMER)
                .thenMany(Flux.error(MySQLExceptionUtils.wrapExceptionIfNeed(e)))
                ;
    }


    /**
     * @return {@link Flux#empty()} or {@link Flux#error(Throwable)} (if terminator is error packet.)
     * @see #receiveResultSetRows(MySQLRowMeta, BiFunction, Consumer)
     * @see #handleResultSetTerminatorOnError(Throwable)
     */
    private <T> Flux<T> handleResultSetTerminator(Consumer<ResultStates> statesConsumer) {
        // use packet decoder , because Text ResultRow decoder do it.
        return this.cumulateReceiver.receiveOnePacket()
                .flatMapMany(packetBuf -> handleResultSetTerminator0(packetBuf, statesConsumer))
                ;
    }

    /**
     * handle Text ResultSet terminator.
     *
     * @return {@link Flux#empty()} or {@link Flux#error(Throwable)} (if terminator is error packet.)
     * @see #handleResultSetTerminator(Consumer)
     */
    private <T> Flux<T> handleResultSetTerminator0(ByteBuf terminatorPacket
            , Consumer<ResultStates> statesConsumer) {
        final int payloadLength = PacketUtils.readInt3(terminatorPacket);
        terminatorPacket.skipBytes(1);// skip sequence_id

        final int negotiatedCapability = obtainNegotiatedCapability();
        final boolean clientDeprecateEof = (negotiatedCapability & ClientProtocol.CLIENT_DEPRECATE_EOF) != 0;
        Flux<T> flux;
        switch (PacketUtils.getInt1(terminatorPacket, terminatorPacket.readerIndex())) {
            case ErrorPacket.ERROR_HEADER:
                // ERROR terminator
                ErrorPacket error;
                error = ErrorPacket.readPacket(terminatorPacket, negotiatedCapability, obtainCharsetResults());
                flux = handleResultSetErrorPacket(error);
                break;
            case EofPacket.EOF_HEADER:
                if (clientDeprecateEof && payloadLength < PacketUtils.ENC_3_MAX_VALUE) {
                    //OK terminator
                    OkPacket okPacket = OkPacket.readPacket(terminatorPacket, negotiatedCapability);
                    flux = textResultSetTerminatorConsumer(MySQLResultStates.from(okPacket), statesConsumer);
                    break;
                } else if (!clientDeprecateEof && payloadLength < 6) {
                    // EOF terminator
                    EofPacket eofPacket = EofPacket.readPacket(terminatorPacket, negotiatedCapability);
                    flux = textResultSetTerminatorConsumer(MySQLResultStates.from(eofPacket), statesConsumer);
                    break;
                }
            default:
                // never here ,if Text ResultSet row decoder no bug.
                flux = Flux.error(new ReactiveSQLException(new SQLException("Text ResultSet row decoder error.")));

        }
        return flux;
    }

    /**
     * @return {@link Flux#error(Throwable)}
     * @see #handleResultSetTerminator0(ByteBuf, Consumer)
     */
    private <T> Flux<T> handleResultSetErrorPacket(ErrorPacket error) {
        return Flux.error(new ReactiveSQLException(
                new SQLException(error.getErrorMessage(), error.getSqlState())));
    }




    /**
     * @see #commandUpdate(String, Consumer)
     * @see #commandQuery(String, BiFunction, Consumer)
     */
    private Mono<Void> sendCommandPacket(String command) {
        // 1. create COM_QUERY packet.
        ByteBuf packetBuf = createPacketBuffer(command.length() * obtainMaxBytesPerCharClient());
        packetBuf.writeByte(PacketUtils.COM_QUERY_HEADER)
                .writeBytes(command.getBytes(obtainCharsetClient()));
        return sendPacket(packetBuf, null);
    }


    /*################################## blow private static method  ##################################*/







    private static <T> Flux<T> textResultSetTerminatorConsumer(ResultStates resultStates
            , Consumer<ResultStates> statesConsumer) {
        Flux<T> flux;
        try {
            statesConsumer.accept(resultStates);
            flux = Flux.empty();
        } catch (Throwable e) {
            flux = Flux.error(new ReactiveSQLException(new SQLException("statesConsumer throw exception.", e)));
        }
        return flux;
    }


}
