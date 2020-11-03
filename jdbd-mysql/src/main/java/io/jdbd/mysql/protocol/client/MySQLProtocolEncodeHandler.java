package io.jdbd.mysql.protocol.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import reactor.netty.Connection;

final class MySQLProtocolEncodeHandler extends ChannelOutboundHandlerAdapter {


    static void addMySQLEncodeHandler(Connection connection) {
        connection.addHandlerLast(MySQLProtocolEncodeHandler.class.getSimpleName(), new MySQLProtocolEncodeHandler());
    }

    private MySQLProtocolEncodeHandler() {

    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof ByteBuf) {
            ByteBuf packetBuf = (ByteBuf) msg;
            PacketUtils.writePacketHeader(packetBuf, 0);
            ctx.write(packetBuf.asReadOnly(), promise);
        } else {
            super.write(ctx, msg, promise);
        }
    }


}
