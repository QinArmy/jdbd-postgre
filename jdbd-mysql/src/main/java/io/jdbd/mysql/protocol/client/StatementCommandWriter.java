package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.BindValue;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;

import java.sql.SQLException;
import java.util.List;

interface StatementCommandWriter {

    Publisher<ByteBuf> writeCommand(List<BindValue> parameterGroup) throws SQLException;


}
