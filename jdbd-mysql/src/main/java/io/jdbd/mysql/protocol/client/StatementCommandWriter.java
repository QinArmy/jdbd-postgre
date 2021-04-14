package io.jdbd.mysql.protocol.client;

import io.jdbd.vendor.statement.ParamValue;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.sql.SQLException;
import java.util.List;

interface StatementCommandWriter {

    Publisher<ByteBuf> writeCommand(int stmtIndex, List<? extends ParamValue> parameterGroup)
            throws SQLException;


    interface LongParameterWriter {

        Flux<ByteBuf> write(int stmtIndex, List<? extends ParamValue> valueList);
    }


}
