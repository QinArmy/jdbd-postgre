package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.ProtocolUtils;
import io.netty.buffer.ByteBuf;

abstract class ClientProtocolUtils extends ProtocolUtils {

    ClientProtocolUtils() {
        throw new UnsupportedOperationException();
    }


    static PacketHeader getHeader(ByteBuf byteBuf) {
        int index = byteBuf.readerIndex();
        return new PacketHeader(DataTypeUtils.getInt3(byteBuf, index)
                , DataTypeUtils.getInt1(byteBuf, index + 3));
    }


     static PacketHeader readHeader(ByteBuf byteBuf) {
        return new PacketHeader(DataTypeUtils.readInt3(byteBuf)
                , DataTypeUtils.readInt1(byteBuf));
    }

}
