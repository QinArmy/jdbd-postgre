package io.jdbd.mysql.protocol;

import java.math.BigInteger;

public abstract class ProtocolDataUtils {

    protected ProtocolDataUtils() {
        throw new UnsupportedOperationException();
    }

    public static short readInt1(byte[] bytes, int offset) {
        if (offset > -1 && offset < bytes.length) {
            return (short) (bytes[offset] & 0xff);
        }
        throw new IndexOutOfBoundsException(String.format("offset[%s] out of bounds[%s]", offset, bytes.length));
    }

    public static int readInt2(byte[] bytes, final int offset) {
        if (offset > -1 && (offset + 1) < bytes.length) {
            return (bytes[offset] & 0xff)
                    | ((bytes[offset + 1] & 0xff) << 8);
        }
        throw new IndexOutOfBoundsException(String.format("offset[%s] out of bounds[%s]", offset, bytes.length));
    }

    public static int readInt3(byte[] bytes, final int offset) {
        if (offset > -1 && (offset + 2) < bytes.length) {
            int position = offset;
            return (bytes[position++] & 0xff)
                    | ((bytes[position++] & 0xff) << 8)
                    | ((bytes[position] & 0xff) << 16);
        }
        throw new IndexOutOfBoundsException(String.format("offset[%s] out of bounds[%s]", offset, bytes.length));
    }

    public static long readInt4(byte[] bytes, final int offset) {
        if (offset > -1 && (offset + 3) < bytes.length) {
            int position = offset;
            return ((long) bytes[position++] & 0xff)
                    | (((long) bytes[position++] & 0xff) << 8)
                    | (((long) bytes[position++] & 0xff) << 16)
                    | (((long) bytes[position] & 0xff) << 24);
        }
        throw new IndexOutOfBoundsException(String.format("offset[%s] out of bounds[%s]", offset, bytes.length));
    }

    public long readInt6(byte[] bytes, final int offset) {
        if (offset > -1 && (offset + 5) < bytes.length) {
            int position = offset;
            return ((long) bytes[position++] & 0xff)
                    | (((long) bytes[position++] & 0xff) << 8)
                    | (((long) bytes[position++] & 0xff) << 16)
                    | (((long) bytes[position++] & 0xff) << 24)
                    | (((long) bytes[position++] & 0xff) << 32)
                    | (((long) bytes[position] & 0xff) << 40);
        }
        throw new IndexOutOfBoundsException(String.format("offset[%s] out of bounds[%s]", offset, bytes.length));
    }

    public static BigInteger readInt8(byte[] bytes, final int offset) {
        if (offset > -1 && (offset + 7) < bytes.length) {
            byte[] numBytes = new byte[8];
            for (int i = 0, position = offset + 7; i < numBytes.length; i++, position--) {
                numBytes[i] = bytes[position];
            }
            return new BigInteger(numBytes);
        }
        throw new IndexOutOfBoundsException(String.format("offset[%s] out of bounds[%s]", offset, bytes.length));
    }

    /**
     *  see {@code com.mysql.cj.protocol.a.NativePacketPayload#readInteger(com.mysql.cj.protocol.a.NativeConstants.IntegerDataType)}
     */
    public BigInteger readLengthEncoded(byte[] bytes, final int offset){
        if (offset > -1 && offset < bytes.length) {
           final int sw = bytes[offset] & 0xff;
            BigInteger encodedNum;
            switch (sw){
                case 251:
                    // represents a NULL in a ProtocolText::ResultsetRow
                    encodedNum = BigInteger.valueOf(-1L);
                    break;
                case 252:
                    encodedNum = BigInteger.valueOf(readInt2(bytes,offset + 1));
                    break;
                case 253:
                    encodedNum = BigInteger.valueOf(readInt3(bytes,offset + 1));
                    break;
                case 254:
                    encodedNum = readInt8(bytes,offset + 1);
                    break;
                default:
                    encodedNum = BigInteger.valueOf(sw);

            }
            return encodedNum;
        }
        throw new IndexOutOfBoundsException(String.format("offset[%s] out of bounds[%s]", offset, bytes.length));
    }

    public static IndexOutOfBoundsException createIndexOutOfBoundsException(int bound,int offset){
       return new IndexOutOfBoundsException(String.format("offset[%s] out of bounds[%s]", offset, bound));
    }



}
