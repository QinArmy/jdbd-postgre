package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;

interface CommandWriter {


    Publisher<ByteBuf> writeCommand(int batchIndex) throws JdbdException;


}
