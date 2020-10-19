package io.jdbd.mysql.protocol.client;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

public abstract class ByteBufferUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ByteBufferUtils.class);

    protected ByteBufferUtils() {
        throw new UnsupportedOperationException();
    }

    public static ByteBuf mergeByteBuf(List<ByteBuf> byteBufList) {
        Iterator<ByteBuf> iterator = byteBufList.iterator();
        ByteBuf firstBuf;

        firstBuf = iterator.next();
        LOG.info("byteBufList site:{},readable bytes:{}",byteBufList.size(),firstBuf.readableBytes());
        while (iterator.hasNext()) {
            firstBuf.writeBytes(iterator.next());
        }
        LOG.info("firstBuf readable bytes:{}",firstBuf.readableBytes());
        return firstBuf;
    }
}
