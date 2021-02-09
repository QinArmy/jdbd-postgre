package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.JdbdMySQLException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.util.annotation.Nullable;

import java.math.BigInteger;
import java.nio.charset.Charset;

public abstract class PacketUtils {


    public static final long NULL_LENGTH = -1L;

    public static final int HEADER_SIZE = 4;

    public static final int MAX_PACKET_CAPACITY = HEADER_SIZE + ClientProtocol.MAX_PACKET_SIZE;

    /**
     * @see #ENC_3
     */
    public static final int ENC_3_MAX_VALUE = 0xFF_FF_FF;

    public static final int LOCAL_INFILE = 0xFB;
    public static final int COM_QUERY_HEADER = 3;
    public static final int COM_STMT_PREPARE = 0x16;
    public static final int COM_STMT_EXECUTE = 0x17;

    public static final int COM_STMT_SEND_LONG_DATA = 0x18;

    public static final int COM_STMT_RESET = 0x1A;
    public static final int COM_QUIT_HEADER = 0x01;


    public static final int ENC_0 = 0xFB;
    public static final int ENC_3 = 0xFC;
    public static final int ENC_4 = 0xFD;
    public static final int ENC_9 = 0xFE;

    public static final int BIT_8 = 0xff;

    public static final long BIT_8L = 0xffL;

    public static final long BIT_32 = 0xffff_ffffL;


    public static short readInt1(ByteBuf byteBuf) {
        return (short) (byteBuf.readByte() & BIT_8);
    }

    public static short getInt1(ByteBuf byteBuf, int index) {
        return (short) (byteBuf.getByte(index) & BIT_8);
    }

    public static int readInt2(ByteBuf byteBuf) {
        return (byteBuf.readByte() & BIT_8)
                | ((byteBuf.readByte() & BIT_8) << 8);
    }

    public static int getInt2(ByteBuf byteBuf, int index) {
        return (byteBuf.getByte(index++) & BIT_8)
                | ((byteBuf.getByte(index) & BIT_8) << 8);
    }

    public static int readInt3(ByteBuf byteBuf) {
        return (byteBuf.readByte() & BIT_8)
                | ((byteBuf.readByte() & BIT_8) << 8)
                | ((byteBuf.readByte() & BIT_8) << 16)
                ;
    }

    public static int getInt3(ByteBuf byteBuf, int index) {
        return (byteBuf.getByte(index++) & BIT_8)
                | ((byteBuf.getByte(index++) & BIT_8) << 8)
                | ((byteBuf.getByte(index) & BIT_8) << 16)
                ;
    }

    public static long readInt4AsLong(ByteBuf byteBuf) {
        return readInt4(byteBuf) & BIT_32;
    }

    public static int readInt4(ByteBuf byteBuf) {
        return (byteBuf.readByte() & BIT_8)
                | ((byteBuf.readByte() & BIT_8) << 8)
                | ((byteBuf.readByte() & BIT_8) << 16)
                | ((byteBuf.readByte() & BIT_8) << 24)
                ;
    }

    public static long getInt4AsLong(ByteBuf byteBuf, int index) {
        return getInt4(byteBuf, index) & BIT_32;
    }


    public static int getInt4(ByteBuf byteBuf, int index) {
        return (byteBuf.getByte(index++) & BIT_8)
                | ((byteBuf.getByte(index++) & BIT_8) << 8)
                | ((byteBuf.getByte(index++) & BIT_8) << 16)
                | ((byteBuf.getByte(index) & BIT_8) << 24)
                ;
    }

    public static long readInt6(ByteBuf byteBuf) {
        return (byteBuf.readByte() & BIT_8L)
                | ((byteBuf.readByte() & BIT_8L) << 8)
                | ((byteBuf.readByte() & BIT_8L) << 16)
                | ((byteBuf.readByte() & BIT_8L) << 24)
                | ((byteBuf.readByte() & BIT_8L) << 32)
                | ((byteBuf.readByte() & BIT_8L) << 40)
                ;
    }

    public static long getInt6(ByteBuf byteBuf, int index) {
        return (byteBuf.getByte(index++) & BIT_8L)
                | ((byteBuf.getByte(index++) & BIT_8L) << 8)
                | ((byteBuf.getByte(index++) & BIT_8L) << 16)
                | ((byteBuf.getByte(index++) & BIT_8L) << 24)
                | ((byteBuf.getByte(index++) & BIT_8L) << 32)
                | ((byteBuf.getByte(index) & BIT_8L) << 40)
                ;
    }

    public static long readInt8(ByteBuf byteBuf) {
        return (byteBuf.readByte() & BIT_8L)
                | ((byteBuf.readByte() & BIT_8L) << 8)
                | ((byteBuf.readByte() & BIT_8L) << 16)
                | ((byteBuf.readByte() & BIT_8L) << 24)
                | ((byteBuf.readByte() & BIT_8L) << 32)
                | ((byteBuf.readByte() & BIT_8L) << 40)
                | ((byteBuf.readByte() & BIT_8L) << 48)
                | ((byteBuf.readByte() & BIT_8L) << 56)
                ;
    }

    public static long getInt8(ByteBuf byteBuf, int index) {
        return (byteBuf.getByte(index++) & BIT_8L)
                | ((byteBuf.getByte(index++) & BIT_8L) << 8)
                | ((byteBuf.getByte(index++) & BIT_8L) << 16)
                | ((byteBuf.getByte(index++) & BIT_8L) << 24)
                | ((byteBuf.getByte(index++) & BIT_8L) << 32)
                | ((byteBuf.getByte(index++) & BIT_8L) << 40)
                | ((byteBuf.getByte(index++) & BIT_8L) << 48)
                | ((byteBuf.getByte(index) & BIT_8L) << 56)
                ;
    }

    /**
     * see {@code com.mysql.cj.protocol.a.NativePacketPayload#readInteger(com.mysql.cj.protocol.a.NativeConstants.IntegerDataType)}
     */
    public static long getLenEnc(ByteBuf byteBuf, int index) {
        final int sw = getInt1(byteBuf, index++);
        long int8;
        switch (sw) {
            case ENC_0:
                // represents a NULL in a ProtocolText::ResultsetRow
                int8 = NULL_LENGTH;
                break;
            case ENC_3:
                int8 = getInt2(byteBuf, index);
                break;
            case ENC_4:
                int8 = getInt3(byteBuf, index);
                break;
            case ENC_9:
                int8 = getInt8(byteBuf, index);
                break;
            default:
                // ENC_1
                int8 = sw;

        }
        return int8;
    }

    public static int obtainLenEncIntByteCount(ByteBuf byteBuf, final int index) {
        int byteCount;
        switch (getInt1(byteBuf, index)) {
            case ENC_0:
                // represents a NULL in a ProtocolText::ResultsetRow
                byteCount = 0;
                break;
            case ENC_3:
                byteCount = 3;
                break;
            case ENC_4:
                byteCount = 4;
                break;
            case ENC_9:
                byteCount = 9;
                break;
            default:
                // ENC_1
                byteCount = 1;
        }
        return byteCount;
    }

    /**
     * see {@code com.mysql.cj.protocol.a.NativePacketPayload#readInteger(com.mysql.cj.protocol.a.NativeConstants.IntegerDataType)}
     */
    public static long readLenEnc(ByteBuf byteBuf) {
        final int sw = readInt1(byteBuf);
        long int8;
        switch (sw) {
            case ENC_0:
                // represents a NULL in a ProtocolText::ResultsetRow
                int8 = NULL_LENGTH;
                break;
            case ENC_3:
                int8 = readInt2(byteBuf);
                break;
            case ENC_4:
                int8 = readInt3(byteBuf);
                break;
            case ENC_9:
                int8 = readInt8(byteBuf);
                break;
            default:
                // ENC_1
                int8 = sw;

        }
        return int8;
    }

    /**
     * see {@code com.mysql.cj.protocol.a.NativePacketPayload#readInteger(com.mysql.cj.protocol.a.NativeConstants.IntegerDataType)}
     */
    public static int readLenEncAsInt(ByteBuf byteBuf) {
        long intEnc = readLenEnc(byteBuf);
        if (intEnc > Integer.MAX_VALUE) {
            throw new JdbdMySQLException("int<lenenc> is long");
        }
        return (int) intEnc;
    }


    /**
     * Protocol::NulTerminatedString
     * Strings that are terminated by a [00] byte.
     */
    public static String readStringTerm(ByteBuf byteBuf, Charset charset) {
        return new String(readStringTermBytes(byteBuf), charset);
    }

    public static byte[] readStringTermBytes(ByteBuf byteBuf) {
        int index = byteBuf.readerIndex();
        int end = byteBuf.writerIndex();
        while ((index < end) && (byteBuf.getByte(index) != 0)) {
            index++;
        }
        if (index >= end) {
            throw new IndexOutOfBoundsException(String.format("not found [00] byte,index:%s,writerIndex:%s", index, end));
        }
        byte[] bytes = new byte[index - byteBuf.readerIndex()];
        byteBuf.readBytes(bytes);
        byteBuf.readByte();// skip terminating byte
        return bytes;
    }


    public static String readStringFixed(ByteBuf byteBuf, int len, Charset charset) {
        byte[] bytes = new byte[len];
        byteBuf.readBytes(bytes);
        return new String(bytes, charset);
    }

    /**
     * Protocol::RestOfPacketString
     * If a string is the last component of a packet, its length can be calculated from the overall packet length minus the current position.
     */
    public static String readStringEof(ByteBuf byteBuf, int payloadLength, Charset charset) {
        // byteBuf is full packet.
        return readStringFixed(byteBuf, payloadLength, charset);
    }

    public static String readStringEof(ByteBuf byteBuf, Charset charset) {
        return readStringFixed(byteBuf, byteBuf.readableBytes(), charset);
    }

    /**
     * Protocol::LengthEncodedString
     * A length encoded string is a string that is prefixed with length encoded integer describing the length of the string.
     * It is a special case of Protocol::VariableLengthString
     */
    @Nullable
    public static String readStringLenEnc(ByteBuf byteBuf, Charset charset) {
        long len = readLenEnc(byteBuf);
        String str;
        if (len == NULL_LENGTH) {
            str = null;
        } else if (len == 0L) {
            str = "";
        } else {
            str = readStringFixed(byteBuf, (int) len, charset);
        }
        return str;
    }


    public static ByteBuf createPacketBuffer(ByteBufAllocator allocator, int payloadCapacity) {
        ByteBuf packetBuffer = allocator.buffer(HEADER_SIZE + payloadCapacity);
        // reserve header 4 bytes.
        packetBuffer.writeZero(HEADER_SIZE);
        return packetBuffer;
    }

    /**
     * <p>
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     * </p>
     *
     * @return true ,at least have one packet.
     */
    public static boolean hasOnePacket(ByteBuf byteBuf) {
        int readableBytes = byteBuf.readableBytes();
        return readableBytes > HEADER_SIZE
                && (readableBytes >= HEADER_SIZE + getInt3(byteBuf, byteBuf.readerIndex()));
    }

    public static boolean hasPacket(ByteBuf cumulateBuffer, final int expectedPacketCount) {
        if (expectedPacketCount < 1) {
            throw new IllegalArgumentException("expectedPacketCount must great than 0");
        }
        final int originalReaderIndex = cumulateBuffer.readerIndex();
        int packetCount = 0;
        for (int readableBytes, payloadLength; packetCount < expectedPacketCount; ) {
            readableBytes = cumulateBuffer.readableBytes();
            if (readableBytes < HEADER_SIZE) {
                break;
            }
            payloadLength = readInt3(cumulateBuffer);
            if (readableBytes < HEADER_SIZE + payloadLength) {
                break;
            }
            cumulateBuffer.skipBytes(1 + payloadLength);
            packetCount++;
            if (packetCount == expectedPacketCount) {
                break;
            }

        }
        cumulateBuffer.readerIndex(originalReaderIndex);
        return packetCount == expectedPacketCount;
    }


    public static void writePacketHeader(ByteBuf packetBuf, final int sequenceId) {
        final int payloadLen = packetBuf.readableBytes() - HEADER_SIZE;
        if (payloadLen > ClientCommandProtocol.MAX_PACKET_SIZE) {
            throw new IllegalArgumentException(
                    String.format("byteBuffer payload greater than %s.", ClientCommandProtocol.MAX_PACKET_SIZE));
        }
        packetBuf.markWriterIndex();
        packetBuf.writerIndex(packetBuf.readerIndex());

        writeInt3(packetBuf, payloadLen);
        packetBuf.writeByte(sequenceId);

        packetBuf.resetWriterIndex();
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


    public static void writeInt1(ByteBuf byteBuffer, final int int1) {
        byteBuffer.writeByte(int1 & BIT_8);
    }

    public static void writeInt2(ByteBuf byteBuffer, final int int2) {
        byteBuffer.writeByte(int2);
        byteBuffer.writeByte((int2 >> 8));
    }

    public static void writeInt3(ByteBuf byteBuffer, final int int3) {
        byteBuffer.writeByte(int3);
        byteBuffer.writeByte((int3 >> 8));
        byteBuffer.writeByte((int3 >> 16));
    }

    public static void writeInt4(ByteBuf byteBuffer, final int int4) {
        byteBuffer.writeByte(int4);
        byteBuffer.writeByte((int4 >> 8));
        byteBuffer.writeByte((int4 >> 16));
        byteBuffer.writeByte((int4 >> 24));
    }

    public static void writeInt8(ByteBuf byteBuffer, final long int8) {
        byteBuffer.writeByte((byte) int8);
        byteBuffer.writeByte((byte) (int8 >> 8));
        byteBuffer.writeByte((byte) (int8 >> 16));
        byteBuffer.writeByte((byte) (int8 >> 24));

        byteBuffer.writeByte((byte) (int8 >> 32));
        byteBuffer.writeByte((byte) (int8 >> 40));
        byteBuffer.writeByte((byte) (int8 >> 42));
        byteBuffer.writeByte((byte) (int8 >> 56));
    }

    public static void writeInt8(ByteBuf byteBuffer, BigInteger int8) {
        byte[] array = int8.toByteArray();
        byte[] int8Array = new byte[8];
        if (array.length >= int8Array.length) {
            for (int i = 0, j = array.length - 1; i < int8Array.length; i++, j--) {
                int8Array[i] = array[j];
            }
        } else {
            int i = 0;
            for (int j = array.length - 1; i < array.length; i++, j--) {
                int8Array[i] = array[j];
            }
            for (; i < int8Array.length; i++) {
                int8Array[i] = 0;
            }
        }
        byteBuffer.writeBytes(int8Array);
    }

    public static void writeIntLenEnc(ByteBuf packetBuffer, final int intLenEnc) {
        writeIntLenEnc(packetBuffer, intLenEnc & BIT_32);
    }

    public static void writeIntLenEnc(ByteBuf packetBuffer, final long intLenEnc) {
        if (intLenEnc >= 0 && intLenEnc < ENC_0) {
            packetBuffer.writeByte((int) intLenEnc);
        } else if (intLenEnc >= ENC_0 && intLenEnc < (1 << 16)) {
            packetBuffer.writeByte(ENC_3);
            writeInt2(packetBuffer, (int) intLenEnc);
        } else if (intLenEnc >= (1 << 16) && intLenEnc < (1 << 24)) {
            packetBuffer.writeByte(ENC_4);
            writeInt3(packetBuffer, (int) intLenEnc);
        } else {
            // intLenEnc < 0 || intLenEnc >=  (1 << 24)
            packetBuffer.writeByte(ENC_9);
            writeInt8(packetBuffer, intLenEnc);
        }
    }

    public static void writeStringTerm(ByteBuf byteBuffer, byte[] stringBytes) {
        byteBuffer.writeBytes(stringBytes);
        byteBuffer.writeZero(1);
    }

    public static void writeStringLenEnc(ByteBuf packetBuffer, ByteBuf stringBuffer) {
        writeIntLenEnc(packetBuffer, stringBuffer.readableBytes());
        packetBuffer.writeBytes(stringBuffer);
    }

    public static void writeStringLenEnc(ByteBuf packetBuffer, byte[] stringBytes) {
        writeIntLenEnc(packetBuffer, stringBytes.length);
        packetBuffer.writeBytes(stringBytes);
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

    public static boolean isAuthSwitchRequestPacket(ByteBuf payloadBuf) {
        return getInt1(payloadBuf, payloadBuf.readerIndex()) == 0xFE;
    }

    /*################################## blow private static method ##################################*/

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
