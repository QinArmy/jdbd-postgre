package io.jdbd.vendor.util;

import io.netty.buffer.ByteBuf;
import org.qinarmy.util.BufferWrapper;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.Function;

abstract class GenericGeometries {

    GenericGeometries() {
        throw new UnsupportedOperationException();
    }

    static final byte HEADER_LENGTH = 9;


    public static boolean wkbEquals(final byte[] geometryOne, final byte[] geometryTwo) {
        final boolean match;
        if (geometryOne.length != geometryTwo.length) {
            match = false;
        } else {
            final byte oneByteOrder = checkByteOrder(geometryOne[0]);
            final byte towByteOrder = checkByteOrder(geometryTwo[0]);

            if (oneByteOrder == towByteOrder) {
                match = Arrays.equals(geometryOne, geometryTwo);
            } else {
                final WkbType wkbTypeOne = WkbType.resolveWkbType(geometryOne, 0);
                final WkbType wkbTypeTwo = WkbType.resolveWkbType(geometryTwo, 0);
                if (wkbTypeOne != WkbType.POINT && geometryOne.length < HEADER_LENGTH) {
                    throw createWkbLengthError(wkbTypeOne.name(), geometryOne.length, HEADER_LENGTH);
                }
                if (wkbTypeOne == wkbTypeTwo) {
                    if (wkbTypeOne == WkbType.POINT) {
                        match = Geometries.pointReverseEquals(geometryOne, geometryTwo);
                    } else if (!JdbdArrayUtils.reverseEquals(geometryOne, geometryTwo, 5, 4, 1)) {
                        match = false;
                    } else {
                        match = reverseEquals(wkbTypeOne, geometryOne, geometryTwo);
                    }
                } else {
                    match = false;
                }
            }
        }
        return match;
    }


    public static void reverseWkb(final byte[] wkbArray, final int offset) {
        final WkbType wkbType = WkbType.resolveWkbType(wkbArray, offset);
        if (wkbType.code < 1000) {
            Geometries.geometryWkbReverse(wkbType, wkbArray, offset);
        } else {
            throw new IllegalArgumentException(String.format("Unknown WKB type[%s]", wkbType.code));
        }

    }


    protected static byte checkByteOrder(byte byteOrder) {
        if (byteOrder != 0 && byteOrder != 1) {
            throw createIllegalByteOrderError(byteOrder);
        }
        return byteOrder;
    }

    /**
     * @return element count.
     */
    protected static int checkAndReverseHeader(final byte[] wkbArray, int offset, WkbType wkbType, Function<Integer, Integer> function) {
        if (wkbArray.length < 5) {
            throw createIllegalWkbLengthError(wkbArray.length, 5);
        }
        final byte byteOrder = checkByteOrder(wkbArray[offset++]);
        final int wkbTypeCode, elementCount;
        if (byteOrder == 1) {
            wkbTypeCode = JdbdNumberUtils.readIntFromBigEndian(wkbArray, offset, 4);
            offset += 4;
            elementCount = JdbdNumberUtils.readIntFromBigEndian(wkbArray, offset, 4);
        } else {
            wkbTypeCode = JdbdNumberUtils.readIntFromLittleEndian(wkbArray, offset, 4);
            offset += 4;
            elementCount = JdbdNumberUtils.readIntFromLittleEndian(wkbArray, offset, 4);
        }
        if (wkbTypeCode != wkbType.code) {
            throw createWkbTypeNotMatchError(wkbType.name(), wkbTypeCode);
        }

        final int needBytes = HEADER_LENGTH + function.apply(elementCount);

        if (wkbArray.length < needBytes) {
            throw createWkbLengthError(wkbType.name(), wkbArray.length, needBytes);
        }

        wkbArray[0] ^= 1;
        JdbdArrayUtils.reverse(wkbArray, 1, 4, 2);
        return elementCount;
    }

    protected static void writeInt(ByteBuf buffer, int writerIndex, boolean bigEndian, int intNum) {
        buffer.markWriterIndex();
        buffer.writerIndex(writerIndex);
        if (bigEndian) {
            buffer.writeInt(intNum);
        } else {
            buffer.writeIntLE(intNum);
        }
        buffer.resetWriterIndex();
    }

    protected static void writeWkbHeader(ByteBuf buffer, boolean bigEndian, WkbType wkbType) {
        if (bigEndian) {
            buffer.writeByte(0);
            buffer.writeInt(wkbType.code);
        } else {
            buffer.writeByte(1);
            buffer.writeIntLE(wkbType.code);
        }
        buffer.writeZero(4);// placeholder of elementCount
    }


    /**
     * @return count of points.
     */
    protected static int readAndWritePoints(final boolean bigEndian, final int coordinates, final boolean pointText
            , final BufferWrapper inWrapper, final BufferWrapper outWrapper)
            throws IllegalArgumentException {

        if (coordinates < 2 || coordinates > 4) {
            throw createIllegalCoordinatesError(coordinates);
        }
        final byte[] inArray = inWrapper.bufferArray, outArray = outWrapper.bufferArray;
        final ByteBuffer inBuffer = inWrapper.buffer, outBuffer = outWrapper.buffer;

        final int inLimit = inBuffer.limit(), outLimit = outBuffer.limit();

        int codePoint, pointCount = 0, inPosition = inBuffer.position(), outPosition = outBuffer.position();
        topFor:
        for (int tempOutPosition, tempInPosition, pointEndIndex; inPosition < inLimit; ) {
            codePoint = inArray[inPosition];
            if (Character.isWhitespace(codePoint)) {
                inPosition++;
                continue;
            }
            if (outLimit - outPosition < (coordinates << 3)) {
                break;
            }
            tempInPosition = inPosition;
            tempOutPosition = outPosition;
            if (pointText) {
                if (codePoint != '(') {
                    throw new IllegalArgumentException("Not found '(' for point text.");
                }
                tempInPosition++;
            }

            //parse coordinates and write to outArray.
            for (int coor = 1, startIndex, endIndex = tempInPosition - 1, p; coor <= coordinates; coor++) {
                startIndex = -1;
                for (p = endIndex + 1; p < inLimit; p++) {
                    if (!Character.isWhitespace(inArray[p])) {
                        startIndex = p;
                        break;
                    }
                }
                if (startIndex < 0) {
                    break topFor;
                }
                endIndex = -1;
                for (p = startIndex + 1; p < inLimit; p++) {
                    if (coor == coordinates) {
                        // last coordinate.
                        codePoint = inArray[p];
                        if (pointText) {
                            if (codePoint == ')' || Character.isWhitespace(codePoint)) {
                                endIndex = p;
                                break;
                            }
                        } else if (codePoint == ',' || codePoint == ')' || Character.isWhitespace(codePoint)) {
                            endIndex = p;
                            break;
                        }
                    } else if (Character.isWhitespace(inArray[p])) {
                        endIndex = p;
                        break;
                    }
                }
                if (endIndex < 0) {
                    if (p - startIndex > 24) {
                        // non-double number
                        byte[] nonDoubleBytes = Arrays.copyOfRange(inArray, startIndex, p);
                        throw createNonDoubleError(new String(nonDoubleBytes));
                    }
                    break topFor;
                }
                double d = Double.parseDouble(new String(Arrays.copyOfRange(inArray, startIndex, endIndex)));
                JdbdNumberUtils.doubleToEndian(bigEndian, d, outArray, tempOutPosition);
                tempOutPosition += 8;
                tempInPosition = endIndex;
            }// parse coordinates and write to outArray.
            // below find point end index
            pointEndIndex = -1;
            for (int parenthesisCount = 0; tempInPosition < inLimit; tempInPosition++) {
                codePoint = inArray[tempInPosition];
                if (Character.isWhitespace(codePoint)) {
                    continue;
                }
                if (codePoint == ')') {
                    parenthesisCount++;
                    if (pointText) {
                        if (parenthesisCount == 2) {
                            pointEndIndex = tempInPosition;
                            break;
                        }
                    } else {
                        pointEndIndex = tempInPosition;
                        break;
                    }
                } else if (codePoint == ',') {
                    if (pointText && parenthesisCount == 0) {
                        throw new IllegalArgumentException("point text not close.");
                    }
                    pointEndIndex = tempInPosition;
                    break;
                } else {
                    throw new IllegalArgumentException(String.format("point end with %s.", (char) codePoint));
                }
            }
            if (pointEndIndex < 0) {
                break;
            }
            outPosition = tempOutPosition;
            inPosition = pointEndIndex;
            pointCount++;
            if (inArray[pointEndIndex] == ')') {
                break;
            }
        }
        inBuffer.position(inPosition);
        outBuffer.position(outPosition);

        return pointCount;
    }

    /**
     * @return count of LinearRing than read from inWrapper.
     * @see #readAndWritePoints(boolean, int, boolean, BufferWrapper, BufferWrapper)
     */
    protected static int readAndWriteLinearRings(final boolean bigEndian, final int coordinates
            , final BufferWrapper inWrapper, final BufferWrapper outWrapper) {
        if (coordinates < 2 || coordinates > 4) {
            throw createIllegalCoordinatesError(coordinates);
        }
        final byte[] inArray = inWrapper.bufferArray, outArray = outWrapper.bufferArray;
        final ByteBuffer inBuffer = inWrapper.buffer, outBuffer = outWrapper.buffer;

        final int inLimit = inBuffer.limit(), outLimit = outBuffer.limit();
        int codePoint, inPosition = inBuffer.position(), outPosition = outBuffer.position();
        int linearRingCount = 0;
        for (int i = 0, pointCount = 0; inPosition < inLimit; ) {
            codePoint = inArray[inPosition];
            if (Character.isWhitespace(codePoint)) {
                inPosition++;
                continue;
            }
            if (codePoint != '(') {
                throw new IllegalArgumentException("Not found '(' for linestring text.");
            }
            pointCount += readAndWritePoints(bigEndian, coordinates, false, inWrapper, outWrapper);

            inPosition = inWrapper.buffer.position();
            if (inWrapper.buffer.get(inPosition) == ')') {
                linearRingCount++;
            }

        }
        return linearRingCount;
    }


    protected static IllegalArgumentException createIllegalCoordinatesError(int coordinates) {
        return new IllegalArgumentException(String.format("coordinates[%s] error.", coordinates));
    }


    protected static IllegalArgumentException createIllegalByteOrderError(byte byteOrder) {
        return new IllegalArgumentException(String.format("Illegal byteOrder[%s].", byteOrder));
    }

    protected static IllegalArgumentException createIllegalWkbLengthError(long length, long expectLength) {
        return new IllegalArgumentException(String.format("WKB length[%s] but expect min length[%s]."
                , length, expectLength));
    }

    protected static IllegalArgumentException createWkbTypeNotMatchError(String type, int wkbType) {
        return new IllegalArgumentException(String.format("WKB type[%s] and %s not match.", wkbType, type));
    }

    protected static IllegalArgumentException createWkbLengthError(String type, long length, long exceptLength) {
        return new IllegalArgumentException(String.format("WKB length[%s] and %s except length[%s] not match."
                , length, type, exceptLength));
    }

    protected static IllegalArgumentException createUnknownWkbTypeError(int wkbType) {
        return new IllegalArgumentException(String.format("Unknown WKB-Type[%s].", wkbType));
    }

    protected static IllegalArgumentException createOffsetError(int offset, int arrayLength) {
        return new IllegalArgumentException(String.format("offset[%s] not in [0,%s).", offset, arrayLength));
    }

    protected static IllegalArgumentException createIllegalElementCount(int elementCount) {
        return new IllegalArgumentException(String.format("elementCount[%s] error", elementCount));
    }

    protected static IllegalArgumentException createIllegalLinearRingCountError(int linearRingCount) {
        return new IllegalArgumentException(String.format("linearRingCount[%s] error", linearRingCount));
    }

    protected static IllegalArgumentException createIllegalLinearPointCountError(int pointCount) {
        return new IllegalArgumentException(String.format("LinearRing pointCount[%s] error", pointCount));
    }

    protected static IllegalArgumentException createIllegalLineStringPointCountError(int pointCount) {
        return new IllegalArgumentException(String.format("LineString pointCount[%s] error", pointCount));
    }


    protected static IllegalArgumentException createNonLinearRingError(int linearRingIndex) {
        return new IllegalArgumentException(String.format("Polygon LinearRing[%s] isn't LinearRing", linearRingIndex));
    }

    protected static IllegalArgumentException createNonDoubleError(String nonDouble) {
        return new IllegalArgumentException(String.format("%s isn't double number.", nonDouble));
    }


    /*################################## blow private method ##################################*/


    /**
     * @see #wkbEquals(byte[], byte[])
     */
    private static boolean reverseEquals(final WkbType wkbType, final byte[] geometryOne, final byte[] geometryTwo) {
        final int elementCount;
        elementCount = JdbdNumberUtils.readIntFromEndian(geometryOne[0] == 0, geometryOne, 5, 4);
        boolean match;
        switch (wkbType) {
            case MULTI_POINT:
            case LINE_STRING:
                match = JdbdArrayUtils.reverseEquals(geometryOne, geometryTwo, HEADER_LENGTH, 8, elementCount << 1);
                break;
            case POLYGON:
            case MULTI_LINE_STRING:
                match = Geometries.lineStringElementReverseEquals(wkbType, geometryOne, geometryTwo, elementCount);
                break;
            case MULTI_POLYGON:
                match = Geometries.multiPolygonWkbReverseEquals(geometryOne, geometryTwo, elementCount);
                break;
            case GEOMETRY_COLLECTION:
            default:
                throw new IllegalArgumentException(String.format("not support %s now.", wkbType));
        }
        return match;
    }


}
