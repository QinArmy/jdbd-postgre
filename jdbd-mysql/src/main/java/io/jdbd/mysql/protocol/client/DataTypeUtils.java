package io.jdbd.mysql.protocol.client;

import com.oracle.tools.packager.Log;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.charset.Charset;

public abstract class DataTypeUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DataTypeUtils.class);

    public static final long NULL_LENGTH = -1;


    public static short readInt1(ByteBuf byteBuf) {
        return (short) (byteBuf.readByte() & 0xff);
    }

    public static short getInt1(ByteBuf byteBuf, int index) {
        return (short) (byteBuf.getByte(index) & 0xff);
    }

    public static int readInt2(ByteBuf byteBuf) {
        if (byteBuf.readableBytes() > 1) {
            return (byteBuf.readByte() & 0xff)
                    | ((byteBuf.readByte() & 0xff) << 8);
        }
        throw createIndexOutOfBoundsException(byteBuf.readableBytes(), 2);
    }

    public static int getInt2(ByteBuf byteBuf, int index) {
        if (byteBuf.readableBytes() > 1) {
            return (byteBuf.getByte(index++) & 0xff)
                    | ((byteBuf.getByte(index) & 0xff) << 8);
        }
        throw createIndexOutOfBoundsException(byteBuf.readableBytes(), 2);
    }

    public static int readInt3(ByteBuf byteBuf) {
        if (byteBuf.readableBytes() > 1) {
            return (byteBuf.readByte() & 0xff)
                    | ((byteBuf.readByte() & 0xff) << 8)
                    | ((byteBuf.readByte() & 0xff) << 16)
                    ;
        }
        throw createIndexOutOfBoundsException(byteBuf.readableBytes(), 2);
    }

    public static int getInt3(ByteBuf byteBuf, int index) {
        if (byteBuf.readableBytes() > 1) {
            return (byteBuf.getByte(index++) & 0xff)
                    | ((byteBuf.getByte(index++) & 0xff) << 8)
                    | ((byteBuf.getByte(index) & 0xff) << 16)
                    ;
        }
        throw createIndexOutOfBoundsException(byteBuf.readableBytes(), 2);
    }

    public static int readInt4(ByteBuf byteBuf) {
        if (byteBuf.readableBytes() > 1) {
            return (byteBuf.readByte() & 0xff)
                    | ((byteBuf.readByte() & 0xff) << 8)
                    | ((byteBuf.readByte() & 0xff) << 16)
                    | ((byteBuf.readByte() & 0xff) << 24)
                    ;
        }
        throw createIndexOutOfBoundsException(byteBuf.readableBytes(), 2);
    }

    public static int getInt4(ByteBuf byteBuf, int index) {
        if (byteBuf.readableBytes() > 3) {
            return (byteBuf.getByte(index++) & 0xff)
                    | ((byteBuf.getByte(index++) & 0xff) << 8)
                    | ((byteBuf.getByte(index++) & 0xff) << 16)
                    | ((byteBuf.getByte(index) & 0xff) << 24)
                    ;
        }
        throw createIndexOutOfBoundsException(byteBuf.readableBytes(), 2);
    }

    public static long readInt6(ByteBuf byteBuf) {
        if (byteBuf.readableBytes() > 5) {
            return ((long) byteBuf.readByte() & 0xffL)
                    | (((long) byteBuf.readByte() & 0xffL) << 8)
                    | (((long) byteBuf.readByte() & 0xffL) << 16)
                    | (((long) byteBuf.readByte() & 0xffL) << 24)
                    | (((long) byteBuf.readByte() & 0xffL) << 32)
                    | (((long) byteBuf.readByte() & 0xffL) << 40)
                    ;
        }
        throw createIndexOutOfBoundsException(byteBuf.readableBytes(), 5);
    }

    public static long getInt6(ByteBuf byteBuf, int index) {
        if (byteBuf.readableBytes() > 5) {
            return ((long) byteBuf.getByte(index++) & 0xffL)
                    | (((long) byteBuf.getByte(index++) & 0xffL) << 8)
                    | (((long) byteBuf.getByte(index++) & 0xffL) << 16)
                    | (((long) byteBuf.getByte(index++) & 0xffL) << 24)
                    | (((long) byteBuf.getByte(index++) & 0xffL) << 32)
                    | (((long) byteBuf.getByte(index) & 0xffL) << 40)
                    ;
        }
        throw createIndexOutOfBoundsException(byteBuf.readableBytes(), 5);
    }

    public static long readInt8(ByteBuf byteBuf) {
        if (byteBuf.readableBytes() > 7) {
            return ((long) byteBuf.readByte() & 0xffL)
                    | (((long) byteBuf.readByte() & 0xffL) << 8)
                    | (((long) byteBuf.readByte() & 0xffL) << 16)
                    | (((long) byteBuf.readByte() & 0xffL) << 24)
                    | (((long) byteBuf.readByte() & 0xffL) << 32)
                    | (((long) byteBuf.readByte() & 0xffL) << 40)
                    | (((long) byteBuf.readByte() & 0xffL) << 48)
                    | (((long) byteBuf.readByte() & 0xffL) << 56)
                    ;
        }
        throw createIndexOutOfBoundsException(byteBuf.readableBytes(), 5);
    }

    public static long getInt8(ByteBuf byteBuf, int index) {
        if (byteBuf.readableBytes() > 7) {
            return ((long) byteBuf.getByte(index++) & 0xffL)
                    | (((long) byteBuf.getByte(index++) & 0xffL) << 8)
                    | (((long) byteBuf.getByte(index++) & 0xffL) << 16)
                    | (((long) byteBuf.getByte(index++) & 0xffL) << 24)
                    | (((long) byteBuf.getByte(index++) & 0xffL) << 32)
                    | (((long) byteBuf.getByte(index++) & 0xffL) << 40)
                    | (((long) byteBuf.getByte(index++) & 0xffL) << 48)
                    | (((long) byteBuf.getByte(index) & 0xffL) << 56)
                    ;
        }
        throw createIndexOutOfBoundsException(byteBuf.readableBytes(), 5);
    }

    /**
     * see {@code com.mysql.cj.protocol.a.NativePacketPayload#readInteger(com.mysql.cj.protocol.a.NativeConstants.IntegerDataType)}
     */
    public static long readLenEnc(ByteBuf byteBuf) {
        final int sw = readInt1(byteBuf);
        long int8;
        switch (sw) {
            case 251:
                // represents a NULL in a ProtocolText::ResultsetRow
                int8 = NULL_LENGTH;
                break;
            case 252:
                int8 = readInt2(byteBuf);
                break;
            case 253:
                int8 = readInt3(byteBuf);
                break;
            case 254:
                int8 = readInt8(byteBuf);
                break;
            default:
                int8 = sw;

        }
        return int8;
    }


    /**
     * Protocol::NulTerminatedString
     * Strings that are terminated by a [00] byte.
     */
    public static String readStringTerm(ByteBuf byteBuf, Charset charset) {
        int index = byteBuf.readerIndex();
        int end = byteBuf.writerIndex();
        while ((index < end) && (byteBuf.getByte(index) != 0)) {
            index++;
        }
        if(index >= end){
            throw new IndexOutOfBoundsException(String.format("not found [00] byte,index:%s,writerIndex:%s",index,end));
        }
        byte[] bytes = new byte[index - byteBuf.readerIndex()];
        byteBuf.readBytes(bytes);
        byteBuf.readByte();// skip terminating byte
        return new String(bytes, charset);
    }

    public static String readStringFixed(ByteBuf byteBuf, int len, Charset charset) {
        byte[] bytes = new byte[len];
        byteBuf. readBytes(bytes);
        return new String(bytes, charset);
    }

    /**
     * Protocol::LengthEncodedString
     * A length encoded string is a string that is prefixed with length encoded integer describing the length of the string.
     * It is a special case of Protocol::VariableLengthString
     */
    public static String readStringLenEnc(ByteBuf byteBuf, Charset charset) {
        long len = readLenEnc(byteBuf);
        LOG.info("lenEnc:{},readable bytes:{}",len,byteBuf.readableBytes());
        String str;
        if (len == NULL_LENGTH) {
            str = null;
        } else if (len == 0L) {
            str = "";
        } else {
            str = readStringFixed(byteBuf,(int) len, charset);
        }
        return str;
    }

    public static IndexOutOfBoundsException createIndexOutOfBoundsException(int readableBytes, int needBytes) {
        return new IndexOutOfBoundsException(
                String.format("need %s bytes but readable %s bytes", needBytes, readableBytes));
    }

    public static BigInteger convertInt8ToBigInteger(long int8) {
        BigInteger bigInteger;
        if (int8 < 0) {
            byte[] bytes = new byte[9];
            int index = 0;
            bytes[index++] = 0;
            divideInt8(int8, bytes, index);
            bigInteger = new BigInteger(bytes);
        } else {
            bigInteger = BigInteger.valueOf(int8);
        }
        return bigInteger;
    }

    public static byte[] convertInt8ToMySQLBytes(long int8) {
        byte[] bytes = new byte[8];
        int index = 7;
        bytes[index--] = (byte) (int8 >> 56);
        bytes[index--] = (byte) (int8 >> 48);
        bytes[index--] = (byte) (int8 >> 40);
        bytes[index--] = (byte) (int8 >> 32);

        bytes[index--] = (byte) (int8 >> 24);
        bytes[index--] = (byte) (int8 >> 16);
        bytes[index--] = (byte) (int8 >> 8);
        bytes[index] = (byte) int8;
        return bytes;
    }

    private static void divideInt8(long int8, byte[] bytes, int index) {
        bytes[index++] = (byte) (int8 >> 56);
        bytes[index++] = (byte) (int8 >> 48);
        bytes[index++] = (byte) (int8 >> 40);
        bytes[index++] = (byte) (int8 >> 32);

        bytes[index++] = (byte) (int8 >> 24);
        bytes[index++] = (byte) (int8 >> 16);
        bytes[index++] = (byte) (int8 >> 8);
        bytes[index] = (byte) int8;
    }
}
