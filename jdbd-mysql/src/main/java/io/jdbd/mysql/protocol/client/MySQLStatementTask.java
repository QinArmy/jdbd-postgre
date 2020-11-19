package io.jdbd.mysql.protocol.client;

import io.jdbd.vendor.StatementTask;
import io.netty.buffer.ByteBuf;

interface MySQLStatementTask extends StatementTask<ByteBuf> {

    boolean decodeAndPublish(ByteBuf cumulateBuffer);
}
