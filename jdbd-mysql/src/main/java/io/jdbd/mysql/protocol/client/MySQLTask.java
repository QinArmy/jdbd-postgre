package io.jdbd.mysql.protocol.client;

import io.jdbd.vendor.CommunicationTask;
import io.netty.buffer.ByteBuf;

interface MySQLTask extends CommunicationTask<ByteBuf> {


}
