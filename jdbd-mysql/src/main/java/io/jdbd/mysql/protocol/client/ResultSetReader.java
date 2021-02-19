package io.jdbd.mysql.protocol.client;


import io.jdbd.JdbdException;
import io.netty.buffer.ByteBuf;

import java.util.function.Consumer;

interface ResultSetReader {

    boolean read(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) throws JdbdException;

    boolean hasMoreResults();

    boolean hasMoreFetch();


}
