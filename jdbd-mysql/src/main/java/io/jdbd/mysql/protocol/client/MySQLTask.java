package io.jdbd.mysql.protocol.client;

import io.jdbd.vendor.CommTask;
import io.netty.buffer.ByteBuf;

interface MySQLTask extends CommTask<ByteBuf> {

    int addAndGetSequenceId();

}
