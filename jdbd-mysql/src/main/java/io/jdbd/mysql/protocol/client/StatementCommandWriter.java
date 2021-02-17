package io.jdbd.mysql.protocol.client;

import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;

import java.util.List;

interface StatementCommandWriter {

     Publisher<ByteBuf> writeCommand(List<BindValue> valueList);


}
