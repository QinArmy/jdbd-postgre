package io.jdbd.vendor.util;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public abstract class JdbdBuffers {

    protected JdbdBuffers() {
        throw new UnsupportedOperationException();
    }


    protected static final byte[] UPPER_CASE_HEX_DIGITS = new byte[]{
            (byte) '0', (byte) '1', (byte) '2', (byte) '3'
            , (byte) '4', (byte) '5', (byte) '6', (byte) '7'
            , (byte) '8', (byte) '9', (byte) 'A', (byte) 'B'
            , (byte) 'C', (byte) 'D', (byte) 'E', (byte) 'F'};

    protected static final byte[] LOWER_CASE_HEX_DIGITS = new byte[]{
            (byte) '0', (byte) '1', (byte) '2', (byte) '3'
            , (byte) '4', (byte) '5', (byte) '6', (byte) '7'
            , (byte) '8', (byte) '9', (byte) 'a', (byte) 'b'
            , (byte) 'c', (byte) 'd', (byte) 'e', (byte) 'f'};


    public static String hexEscapesText(boolean upperCase, byte[] bufferArray, final int length) {
        return new String(hexEscapes(upperCase, bufferArray, length), StandardCharsets.US_ASCII);
    }


    public static byte[] hexEscapes(boolean upperCase, byte[] bufferArray, final int length) {
        if (length < 1 || length > bufferArray.length) {
            throw new IllegalArgumentException(
                    String.format("length[%s] not in [1,%s]", length, bufferArray.length));
        }
        final byte[] hexDigits = upperCase ? UPPER_CASE_HEX_DIGITS : LOWER_CASE_HEX_DIGITS;
        final byte[] hexDigitArray = new byte[length << 1];
        for (int i = 0, j = 0; i < length; i++, j += 2) {
            byte b = bufferArray[i];
            hexDigitArray[j] = hexDigits[(b >> 4) & 0xF]; // write highBits
            hexDigitArray[j + 1] = hexDigits[b & 0xF]; // write lowBits
        }
        return hexDigitArray;
    }

    public static byte[] decodeHex(byte[] hexBytes, final int length) {
        if (length < 1 || length > hexBytes.length || (length & 1) != 0) {
            throw new IllegalArgumentException(
                    String.format("length[%s] not in [1,%s]", length, hexBytes.length));
        }
        final byte[] digitArray = new byte[length >> 1];
        final byte num0 = '0', num9 = '9', a = 'a', f = 'f', A = 'A', F = 'F';
        final byte intervalOfa = a - 10, intervalOfA = A - 10;
        for (int i = 0, j = 0; i < digitArray.length; i++, j += 2) {

            for (int k = j; k < j + 2; k++) {
                byte b = hexBytes[k];
                if (b >= num0 && b <= num9) {
                    b -= num0;
                } else if (b >= a && b <= f) {
                    b -= intervalOfa;
                } else if (b >= A && b <= F) {
                    b -= intervalOfA;
                } else {
                    throw new IllegalArgumentException("non-hex");
                }
                digitArray[i] = (byte) ((digitArray[i] << 4) | b);
            }

        }
        return digitArray;
    }


    @Deprecated
    public static void writeUpperCaseHexEscapes(final ByteBuf message, final byte[] bytes, final int length) {
        message.writeBytes(hexEscapes(true, bytes, length));
    }


    public static void cumulateBuffer(final ByteBuffer buffer) {
        if (!buffer.hasRemaining()) {
            buffer.clear();
            return;
        }
        final int remaining = buffer.remaining();
        if (buffer.hasArray()) {
            final byte[] array = buffer.array();
            for (int index = 0, i = buffer.position(), limit = buffer.limit(); i < limit; i++, index++) {
                array[index] = array[i];
            }
        } else {
            for (int index = 0, i = buffer.position(), limit = buffer.limit(); i < limit; i++, index++) {
                buffer.put(index, buffer.get(i));
            }
        }
        buffer.position(0)
                .limit(remaining);
    }


}
