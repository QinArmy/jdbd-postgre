package io.jdbd.mysql.protocol.client;

import io.jdbd.ResultRow;
import io.jdbd.ResultRowMeta;
import io.jdbd.ResultStates;
import io.jdbd.mysql.protocol.conf.HostInfo;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.vendor.ReactorMultiResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;

abstract class AbstractClientProtocol implements ClientProtocol, ClientProtocolAdjutant {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractClientProtocol.class);

    final HostInfo hostInfo;

    final MySQLTaskAdjutant taskAdjutant;

    final Properties properties;

    AbstractClientProtocol(HostInfo hostInfo, MySQLTaskAdjutant taskAdjutant) {
        this.hostInfo = hostInfo;
        this.taskAdjutant = taskAdjutant;
        this.properties = this.hostInfo.getProperties();
    }

    @Override
    public ReactorMultiResults commands(List<String> commandList) {
        return ComQueryTask.commands(this.taskAdjutant, commandList);
    }


    @Override
    public final Mono<Long> commandUpdate(String command, Consumer<ResultStates> statesConsumer) {
        return ComQueryTask.command(this.taskAdjutant, command)
                .nextUpdate(statesConsumer);
    }


    @Override
    public final <T> Flux<T> commandQuery(String command, BiFunction<ResultRow, ResultRowMeta, T> rowDecoder
            , Consumer<ResultStates> statesConsumer) {
        return ComQueryTask.command(this.taskAdjutant, command)
                .nextQuery(rowDecoder, statesConsumer);
    }

    @Override
    public final Mono<Void> closeGracefully() {
        return QuitTask.quit(this.taskAdjutant);
    }

    @Override
    public final HostInfo obtainHostInfo() {
        return this.hostInfo;
    }


}
