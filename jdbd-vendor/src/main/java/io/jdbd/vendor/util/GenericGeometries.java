package io.jdbd.vendor.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.qinarmy.util.BufferWrapper;
import org.qinarmy.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Stack;

abstract class GenericGeometries {

    GenericGeometries() {
        throw new UnsupportedOperationException();
    }

    private static final Logger LOG = LoggerFactory.getLogger(GenericGeometries.class);

    static final byte HEADER_LENGTH = 9;


    public static boolean wkbEquals(final byte[] geometryOne, final byte[] geometryTwo) {
        final boolean match;
        if (geometryOne.length != geometryTwo.length) {
            match = false;
        } else {
            final WkbType wkbTypeOne, wkbTypeTwo;
            wkbTypeOne = WkbType.resolveWkbType(geometryOne, 0);
            wkbTypeTwo = WkbType.resolveWkbType(geometryTwo, 0);
            if (wkbTypeOne != wkbTypeTwo) {
                match = false;
            } else if (geometryOne[0] == geometryTwo[0]) {
                match = Arrays.equals(geometryOne, geometryTwo);
            } else if (isPoint(wkbTypeOne)) {
                if (geometryOne.length == 5) {
                    match = true;
                } else {
                    match = JdbdArrayUtils.reverseEquals(geometryOne, geometryTwo, 5, 8, wkbTypeOne.coordinates());
                }
            } else {
                match = geometryReverseEquals(geometryOne, geometryTwo, wkbTypeOne);
            }
        }
        return match;
    }

    public static int readElementCount(final byte[] wkbArray) {
        WkbType wkbType = WkbType.resolveWkbType(wkbArray, 0);
        final int elementCount;
        switch (wkbType) {
            case POINT:
            case POINT_Z:
            case POINT_M:
            case POINT_ZM: {
                if (wkbArray.length == 5) {
                    elementCount = 0;
                } else if (wkbArray.length == (5 + (wkbType.coordinates() << 3))) {
                    elementCount = 1;
                } else {
                    throw createIllegalWkbLengthError(wkbArray.length, (5 + ((long) wkbType.coordinates() << 3)));
                }
            }
            break;
            default: {
                if (wkbArray.length < HEADER_LENGTH) {
                    throw createIllegalWkbLengthError(wkbArray.length, HEADER_LENGTH);
                } else {
                    elementCount = JdbdNumberUtils.readIntFromEndian(wkbArray[0] == 0, wkbArray, 5, 4);
                }
            }
        }
        return elementCount;
    }


    /**
     * @see #pointToWkt(byte[], int[])
     */
    public static String pointToWkt(final byte[] wkbArray) {
        return pointToWkt(wkbArray, new int[]{0});
    }

    /**
     * @param wkbArray  <ul>
     *                  <li>{@link WkbType#POINT}</li>
     *                  <li>{@link WkbType#POINT_Z}</li>
     *                  <li>{@link WkbType#POINT_M}</li>
     *                  <li>{@link WkbType#POINT_ZM}</li>
     *                  </ul>
     * @param offsetOut array that length is 1 .
     * @return WKT
     */
    public static String pointToWkt(final byte[] wkbArray, final int[] offsetOut) {
        int offset = offsetOut[0];
        if (offset < 0 || offset >= wkbArray.length) {
            throw createOffsetError(offset, wkbArray.length);
        }
        final WkbType wkbType = WkbType.resolveWkbType(wkbArray, offset);
        assertPoint(wkbType);

        if (wkbArray.length - offset == 5) {
            return createEmptySet(wkbType);
        }
        final boolean bigEndian = wkbArray[offset] == 0;
        offset += 5;
        final int coordinates = wkbType.coordinates();
        StringBuilder builder = new StringBuilder(wkbType.wktType.length() + coordinates * 10)
                .append(wkbType.wktType)
                .append("(");
        for (int i = 0; i < coordinates; i++) {
            if (i > 0) {
                builder.append(" ");
            }
            builder.append(JdbdNumberUtils.readDoubleFromEndian(bigEndian, wkbArray, offset, 8));
            offset += 8;
        }
        builder.append(")");
        return builder.toString();
    }

    public static String lineStringToWkt(final byte[] wkbArray) {
        return lineStringToWkt(wkbArray, new int[]{0});
    }


    /**
     * @param wkbArray  <ul>
     *                  <li>LineString</li>
     *                  <li>LineString Z</li>
     *                  <li>LineString M</li>
     *                  <li>LineString ZM</li>
     *                  </ul>
     * @param offsetOut array that length is 1.
     */
    public static String lineStringToWkt(final byte[] wkbArray, final int[] offsetOut) {
        int offset = offsetOut[0];
        if (offset < 0 || offset >= wkbArray.length) {
            throw createOffsetError(offset, wkbArray.length);
        }
        final WkbType wkbType = WkbType.resolveWkbType(wkbArray, offset);

        if (wkbType != WkbType.LINE_STRING) {
            throw new IllegalArgumentException(String.format("Unsupported WKB[%s] type.", wkbType));
        }
        if (wkbArray.length - offset < HEADER_LENGTH) {
            throw createWkbLengthError(wkbType.wktType, wkbArray.length, offset + HEADER_LENGTH);
        }
        final boolean bigEndian = checkByteOrder(wkbArray[offset]) == 0;
        offset += 5;
        final int pointCount = JdbdNumberUtils.readIntFromEndian(bigEndian, wkbArray, offset, 4);
        if (pointCount < 0) {
            throw createIllegalElementCount(pointCount);
        } else if (pointCount == 0) {
            return wkbType.wktType + " EMPTY";
        }
        offset += 4;
        final int coordinates = wkbType.coordinates();
        final int needBytes = coordinates * 8 * pointCount;
        if (wkbArray.length - offset < needBytes) {
            throw createWkbLengthError(wkbType.wktType, wkbArray.length, offset + needBytes);
        }
        StringBuilder builder = new StringBuilder(coordinates * pointCount * 8)
                .append(wkbType.wktType)
                .append("(");
        for (int i = 0; i < pointCount; i++) {
            if (i > 0) {
                builder.append(",");
            }
            for (int j = 0; j < coordinates; j++) {
                if (j > 0) {
                    builder.append(" ");
                }
                builder.append(JdbdNumberUtils.readDoubleFromEndian(bigEndian, wkbArray, offset, 8));
                offset += 8;
            }
        }
        builder.append(")");
        return builder.toString();
    }

    public static String multiPointToWkt(final byte[] wkbArray) {
        return multiPointToWkt(wkbArray, new int[]{0});
    }

    /**
     * @param wkbArray  <ul>
     *                  <li>MULTI_POINT</li>
     *                  <li>MULTI_POINT Z</li>
     *                  <li>MULTI_POINT M</li>
     *                  <li>MULTI_POINT ZM</li>
     *                  </ul>
     * @param offsetOut array than length is 1.
     */
    public static String multiPointToWkt(final byte[] wkbArray, int[] offsetOut) {
        int offset = offsetOut[0];
        if (offset < 0 || offset >= wkbArray.length) {
            throw createOffsetError(offset, wkbArray.length);
        }
        final WkbType wkbType = WkbType.resolveWkbType(wkbArray, offset);

        if (wkbType != WkbType.MULTI_POINT) {
            throw new IllegalArgumentException(String.format("Unsupported WKB[%s] type.", wkbType));
        }
        if (wkbArray.length - offset < HEADER_LENGTH) {
            throw createWkbLengthError(wkbType.wktType, wkbArray.length, offset + HEADER_LENGTH);
        }
        boolean bigEndian = checkByteOrder(wkbArray[offset]) == 0;
        offset += 5;
        final int pointCount = JdbdNumberUtils.readIntFromEndian(bigEndian, wkbArray, offset, 4);
        offset += 4;
        if (pointCount < 0) {
            throw createIllegalElementCount(pointCount);
        } else if (pointCount == 0) {
            offsetOut[0] = offset;
            return wkbType.wktType + " EMPTY";
        }
        final int coordinates = wkbType.coordinates();
        final int needBytes = (5 + (coordinates * 8)) * pointCount;
        if (wkbArray.length - offset < needBytes) {
            throw createWkbLengthError(wkbType.wktType, wkbArray.length, offset + needBytes);
        }
        StringBuilder builder = new StringBuilder(coordinates * 8 * pointCount)
                .append(wkbType.wktType)
                .append("(");
        // output MultiPoint
        final WkbType elementType = wkbType.elementType();
        WkbType pointType;
        for (int i = 0; i < pointCount; i++) {
            if (i > 0) {
                builder.append(",");
            }
            pointType = WkbType.resolveWkbType(wkbArray, offset);
            if (pointType != elementType) {
                throw new IllegalArgumentException(String.format("Error element type[%s],should be type[%s]."
                        , pointType, elementType));
            }
            bigEndian = wkbArray[offset] == 0;
            offset += 5;
            // output point text
            builder.append("(");
            for (int j = 0; j < coordinates; j++) {
                if (j > 0) {
                    builder.append(" ");
                }
                builder.append(JdbdNumberUtils.readDoubleFromEndian(bigEndian, wkbArray, offset, 8));
                offset += 8;
            }
            builder.append(")");
        }
        builder.append(")");
        offsetOut[0] = offset;
        return builder.toString();
    }

    public static String multiLineStringToWkt(final byte[] wkbArray) {
        return multiLineStringToWkt(wkbArray, new int[]{0});
    }

    /**
     * @param wkbArray  <ul>
     *                  <li>MULTI_LINE_STRING</li>
     *                  <li>MULTI_LINE_STRING Z</li>
     *                  <li>MULTI_LINE_STRING M</li>
     *                  <li>MULTI_LINE_STRING ZM</li>
     *                  </ul>
     * @param offsetOut array than length is 1.
     */
    public static String multiLineStringToWkt(final byte[] wkbArray, final int[] offsetOut) {
        int offset = offsetOut[0];
        if (offset < 0 || offset >= wkbArray.length) {
            throw createOffsetError(offset, wkbArray.length);
        }
        final WkbType wkbType = WkbType.resolveWkbType(wkbArray, offset);

        if (wkbType != WkbType.MULTI_LINE_STRING) {
            throw new IllegalArgumentException(String.format("Unsupported WKB[%s] type.", wkbType));
        }
        if (wkbArray.length - offset < HEADER_LENGTH) {
            throw createWkbLengthError(wkbType.wktType, wkbArray.length, offset + HEADER_LENGTH);
        }
        boolean bigEndian = checkByteOrder(wkbArray[offset]) == 0;
        offset += 5;
        final int lineStringCount = JdbdNumberUtils.readIntFromEndian(bigEndian, wkbArray, offset, 4);
        offset += 4;
        if (lineStringCount < 0) {
            throw createIllegalElementCount(lineStringCount);
        } else if (lineStringCount == 0) {
            offsetOut[0] = offset;
            return wkbType.wktType + " EMPTY";
        }
        final int coordinates = wkbType.coordinates();
        StringBuilder builder = new StringBuilder((5 + (coordinates * 8) * 2) * lineStringCount)
                .append(wkbType.wktType)
                .append("(");
        // output MultiPoint
        final WkbType elementType = wkbType.elementType();
        WkbType lineStringType;
        for (int i = 0, pointCount; i < lineStringCount; i++) {
            if (i > 0) {
                builder.append(",");
            }
            lineStringType = WkbType.resolveWkbType(wkbArray, offset);
            if (lineStringType != elementType) {
                throw new IllegalArgumentException(String.format("Error element type[%s],should be type[%s]."
                        , lineStringType, elementType));
            }
            bigEndian = wkbArray[offset] == 0;
            offset += 5;
            pointCount = JdbdNumberUtils.readIntFromEndian(bigEndian, wkbArray, offset, 4);
            offset += 4;
            // output LineString text
            builder.append("(");
            for (int j = 0; j < pointCount; j++) {
                if (j > 0) {
                    builder.append(",");
                }
                for (int k = 0; k < coordinates; k++) {
                    if (k > 0) {
                        builder.append(" ");
                    }
                    builder.append(JdbdNumberUtils.readDoubleFromEndian(bigEndian, wkbArray, offset, 8));
                    offset += 8;
                }
            }
            builder.append(")");
        }
        builder.append(")");
        offsetOut[0] = offset;
        return builder.toString();
    }

    /*################################## blow protected method ##################################*/

    /**
     * @param wkbType <ul>
     *                <li>{@link WkbType#POINT}</li>
     *                <li>{@link WkbType#POINT_Z}</li>
     *                <li>{@link WkbType#POINT_M}</li>
     *                <li>{@link WkbType#POINT_ZM}</li>
     *                </ul>
     */
    protected static void doPointToWkb(final BufferWrapper inWrapper, final WkbOUtWrapper outWrapper
            , final WkbType wkbType) {
        assertPoint(wkbType);

        if (!readWktType(inWrapper, wkbType.wktType)) {
            throw createWktFormatError(wkbType.wktType);
        }

        outWrapper.bufferArray[0] = outWrapper.bigEndian ? (byte) 0 : (byte) 1;
        JdbdNumberUtils.intToEndian(outWrapper.bigEndian, wkbType.code, outWrapper.bufferArray, 1, 4);
        outWrapper.buffer.position(5);

        if (isEmptySet(inWrapper)) {
            return;
        }
        inWrapper.buffer.get(); //skip point left parenthesis .

        if (readAndWritePoints(inWrapper, outWrapper, wkbType) != 1 || inWrapper.buffer.get() != ')') {
            throw createWktFormatError(wkbType.wktType);
        }

    }

    /**
     * @param wkbType <ul>
     *                <li>LineString</li>
     *                <li>LineString Z</li>
     *                <li>LineString M</li>
     *                <li>LineString ZM</li>
     *                </ul>
     */
    protected static void doLineStringToWkb(final BufferWrapper inWrapper, final WkbMemoryWrapper outWrapper
            , final WkbType wkbType) {

        switch (wkbType) {
            case LINE_STRING:
            case LINE_STRING_Z:
            case LINE_STRING_M:
            case LINE_STRING_ZM:
                break;
            default:
                throw new IllegalArgumentException(String.format("Unsupported WKB type[%s]", wkbType));
        }
        final int writerIndex = doGeometryHeaderToWkb(inWrapper, outWrapper, wkbType);
        final int pointCount;

        pointCount = lineStringTextToWkb(inWrapper, outWrapper, wkbType);
        if (pointCount != 0) {
            writeInt(outWrapper.outChannel, writerIndex, outWrapper.bigEndian, pointCount);
        }

    }

    protected static int lineStringTextToWkb(final BufferWrapper inWrapper, final WkbMemoryWrapper outWrapper
            , final WkbType wkbType) {

        if (isEmptySet(inWrapper)) {
            return 0;
        }
        final ByteBuffer inBuffer = inWrapper.buffer, outBuffer = outWrapper.buffer;
        skipWhitespace(inWrapper);
        inBuffer.get(); // skip LineString left parenthesis.

        final ByteBuf outChannel = outWrapper.outChannel;
        final int inLimit = inBuffer.limit();

        int pointCount = 0;
        boolean lineStringEnd = false;
        for (int inPosition = inBuffer.position(), outPosition; inPosition < inLimit; ) {
            outPosition = outBuffer.position();
            pointCount += readAndWritePoints(inWrapper, outWrapper, wkbType);
            if (outBuffer.position() == outPosition) {
                throw createWktFormatError(wkbType.wktType);
            }
            outBuffer.flip();
            outChannel.writeBytes(outBuffer);
            outBuffer.clear();

            inPosition = inBuffer.position();
            if (inBuffer.get(inPosition) == ')') {
                lineStringEnd = true;
                inBuffer.get(); // skip
                break;
            }

        }
        if (!lineStringEnd) {
            throw new IllegalArgumentException(String.format("%s not close.", wkbType.wktType));
        }
        if (pointCount < 2) {
            throw new IllegalArgumentException(String.format("%s point count < 2", pointCount));
        }
        return pointCount;
    }

    /**
     * @param wkbType <ul>
     *                <li>MultiPoint</li>
     *                <li>MultiPoint Z</li>
     *                <li>MultiPoint M</li>
     *                <li>MultiPoint ZM</li>
     *                </ul>
     */
    protected static void doMultiPointToWkb(final BufferWrapper inWrapper, final WkbMemoryWrapper outWrapper
            , final WkbType wkbType) {

        //1. read and write MULTI_POINT header WKB
        final int writerIndex = doGeometryHeaderToWkb(inWrapper, outWrapper, wkbType);
        if (isEmptySet(inWrapper)) {
            return;
        }

        final ByteBuffer inBuffer = inWrapper.buffer, outBuffer = outWrapper.buffer;
        final ByteBuf outChannel = outWrapper.outChannel;
        // clear outBuffer
        outBuffer.flip();
        if (outBuffer.hasRemaining()) {
            outChannel.writeBytes(outBuffer);
        }
        outBuffer.clear();

        final int inLimit = inBuffer.limit();
        final byte[] pointArray = new byte[wkbType.coordinates() << 3];

        int pointCount = 0;
        //2. read point
        for (int inPosition = inBuffer.position(), pointNum; inPosition < inLimit; ) {

            pointNum = readAndWritePoints(inWrapper, outWrapper, wkbType);
            if (inBuffer.position() != inPosition) {
                throw createWktFormatError(wkbType.wktType);
            }
            pointCount += pointNum;
            outBuffer.flip();
            for (int j = 0; j < pointNum; j++) {
                if (outWrapper.bigEndian) {
                    outChannel.writeByte(0);
                    outChannel.writeInt(wkbType.elementType().code);
                } else {
                    outChannel.writeByte(1);
                    outChannel.writeIntLE(wkbType.elementType().code);
                }
                outBuffer.get(pointArray);
                outChannel.writeBytes(pointArray);
            }
            outBuffer.clear();
            inPosition = inBuffer.position();
            if (inBuffer.get(inPosition) == ')') {
                inBuffer.get(); //skip left parenthesis.
                break;
            }
        }
        //3. write MULTI_LINE_STRING LineString count.
        writeInt(outWrapper.outChannel, writerIndex, outWrapper.bigEndian, pointCount);
    }

    /**
     * @param wkbType <ul>
     *                <li>MultiLineString</li>
     *                <li>MultiLineString Z</li>
     *                <li>MultiLineString M</li>
     *                <li>MultiLineString ZM</li>
     *                </ul>
     */
    protected static void doMultiLineStringToWkb(final BufferWrapper inWrapper, final WkbMemoryWrapper outWrapper
            , final WkbType wkbType) {
        final ByteBuffer inBuffer = inWrapper.buffer;
        //1. read and write MULTI_LINE_STRING header WKB
        final int elementCountWriterIndex = doGeometryHeaderToWkb(inWrapper, outWrapper, wkbType);
        final byte[] inArray = inWrapper.bufferArray;

        if (isEmptySet(inWrapper)) {
            return;
        }
        int lineStringCount = 0;

        final int inLimit = inBuffer.limit();
        //2. write LineString wkb
        for (int i = 0, inPosition = inBuffer.position() + 1, codePoint, writerIndex, pointCount; inPosition < inLimit; i++) {
            codePoint = inArray[inPosition];
            if (Character.isWhitespace(codePoint)) {
                inPosition++;
                continue;
            }
            if (i > 0) {
                if (codePoint == ')') {
                    // right parenthesis,MultiLineString end.
                    break;
                } else if (codePoint != ',') {
                    // linearRing separator
                    throw createWktFormatError(wkbType.wktType);
                }
                inPosition++;
                for (; inPosition < inLimit; inPosition++) {
                    codePoint = inArray[inPosition];
                    if (!Character.isWhitespace(codePoint)) {
                        break;
                    }
                }
            }
            if (inPosition == inLimit) {
                throw createWktFormatError(wkbType.wktType);
            }
            //2-1 write LineString WKB header.
            writerIndex = writeWkbHeader(outWrapper, wkbType.elementType());
            if (isEmptySet(inWrapper)) {
                inPosition = inBuffer.position();
                lineStringCount++;
                continue;
            }
            if (codePoint != '(') {
                // LinearRing start
                throw createWktFormatError(wkbType.wktType);
            }
            inPosition++;
            inBuffer.position(inPosition);
            //2-2 write POINTS WKB.
            pointCount = readAndWritePoints(inWrapper, outWrapper, wkbType);
            if (inBuffer.get() != ')') {
                throw createWktFormatError(wkbType.wktType);
            }
            //2-3 write point count.
            writeInt(outWrapper.outChannel, writerIndex, outWrapper.bigEndian, pointCount);
            inPosition = inBuffer.position();
            lineStringCount++;

        }
        if (lineStringCount < 1) {
            throw createWktFormatError(wkbType.wktType);
        }
        //3. write MULTI_LINE_STRING LineString count.
        writeInt(outWrapper.outChannel, elementCountWriterIndex, outWrapper.bigEndian, lineStringCount);

    }


    public static String polygonToWkt(final byte[] wkbArray) {
        return polygonToWkt(wkbArray, new int[]{0});
    }


    /**
     * @param wkbArray <ul>
     *                 <li>POLYGON</li>
     *                 <li>POLYGON Z</li>
     *                 <li>POLYGON M</li>
     *                 <li>POLYGON ZM</li>
     *                 </ul>
     */
    public static String polygonToWkt(final byte[] wkbArray, final int[] offsetOut) {
        int offset = offsetOut[0];
        if (offset < 0 || offset >= wkbArray.length) {
            throw createOffsetError(offset, wkbArray.length);
        }
        final WkbType wkbType = WkbType.resolveWkbType(wkbArray, offset);
        if (wkbType != WkbType.POLYGON) {
            throw new IllegalArgumentException(String.format("Unsupported WKB[%s] type.", wkbType));
        }
        if (wkbArray.length - offset < HEADER_LENGTH) {
            throw createWkbLengthError(wkbType.wktType, wkbArray.length, offset + HEADER_LENGTH);
        }
        final boolean bigEndian = checkByteOrder(wkbArray[offset]) == 0;
        offset += 5;
        final int lineStringCount;
        lineStringCount = JdbdNumberUtils.readIntFromEndian(bigEndian, wkbArray, offset, 4);
        if (lineStringCount < 0) {
            throw createIllegalElementCount(lineStringCount);
        } else if (lineStringCount == 0) {
            return wkbType.wktType + " EMPTY";
        }
        offset += 4;
        final int coordinates = wkbType.coordinates();
        StringBuilder builder = new StringBuilder(coordinates * 8 * 4 * lineStringCount)
                .append(wkbType.wktType)
                .append("(");

        final byte[] startPointArray = new byte[coordinates << 3], endPointArray = new byte[startPointArray.length];
        // output polygon
        for (int i = 0, pointCount; i < lineStringCount; i++) {
            if (i > 0) {
                builder.append(",");
            }
            pointCount = JdbdNumberUtils.readIntFromEndian(bigEndian, wkbArray, offset, 4);
            if (pointCount < 4) {
                throw createIllegalLinearPointCountError(pointCount);
            }
            offset += 4;
            if (wkbArray.length - offset < pointCount * startPointArray.length) {
                throw createWkbLengthError(wkbType.wktType, wkbArray.length
                        , offset + (long) pointCount * startPointArray.length);
            }
            builder.append("(");
            // output one LinearRing
            for (int j = 0, last = pointCount - 1; j < pointCount; j++) {
                if (j == 0) {
                    System.arraycopy(wkbArray, offset, startPointArray, 0, startPointArray.length);
                } else {
                    builder.append(",");
                }
                if (j == last) {
                    System.arraycopy(wkbArray, offset, endPointArray, 0, endPointArray.length);
                    if (!Arrays.equals(startPointArray, endPointArray)) {
                        throw createNonLinearRingError(i);
                    }
                }
                // output one point
                for (int k = 0; k < coordinates; k++) {
                    if (k > 0) {
                        builder.append(" ");
                    }
                    builder.append(JdbdNumberUtils.readDoubleFromEndian(bigEndian, wkbArray, offset, 8));
                    offset += 8;
                }
            }
            builder.append(")");
        }
        return builder.append(")")
                .toString();
    }

    /**
     * @param wkbType <ul>
     *                <li>MULTI_POLYGON</li>
     *                <li>MULTI_POLYGON Z</li>
     *                <li>MULTI_POLYGON M</li>
     *                <li>MULTI_POLYGON ZM</li>
     *                </ul>
     */
    protected static void doMultiPolygonToWkb(final BufferWrapper inWrapper, final WkbMemoryWrapper outWrapper
            , final WkbType wkbType) {
        if (wkbType != WkbType.MULTI_POLYGON) {
            throw new IllegalArgumentException(String.format("Unsupported WKB[%s]", wkbType));
        }
        //1. read and write MULTI_POLYGON header WKB
        final int elementCountWriterIndex = doGeometryHeaderToWkb(inWrapper, outWrapper, wkbType);
        final byte[] inArray = inWrapper.bufferArray;
        if (isEmptySet(inWrapper)) {
            return;
        }
        final ByteBuffer inBuffer = inWrapper.buffer, outBuffer = outWrapper.buffer;
        final ByteBuf outChannel = outWrapper.outChannel;
        // clear outBuffer
        outBuffer.flip();
        if (outBuffer.hasRemaining()) {
            outChannel.writeBytes(outBuffer);
        }
        outBuffer.clear();
        inBuffer.get(); // skip MULTI_POLYGON left parenthesis.
        int polygonCount = 0, inPosition = inBuffer.position() + 1;
        final int inLimit = inBuffer.limit();
        //2. write MULTI_POLYGON wkb
        for (int i = 0, codePoint, writerIndex, linearRingCount; inPosition < inLimit; i++) {
            codePoint = inArray[inPosition];
            if (Character.isWhitespace(codePoint)) {
                inPosition++;
                continue;
            }
            if (i > 0) {
                if (codePoint == ')') {
                    // right parenthesis,MULTI_POLYGON end.
                    break;
                } else if (codePoint != ',') {
                    // POLYGON separator
                    throw createWktFormatError(wkbType.wktType);
                }
                inPosition++;
                for (; inPosition < inLimit; inPosition++) {
                    codePoint = inArray[inPosition];
                    if (!Character.isWhitespace(codePoint)) {
                        break;
                    }
                }
            }
            if (inPosition == inLimit) {
                throw createWktFormatError(wkbType.wktType);
            }
            if (codePoint != '(') {
                // LinearRing start
                throw createWktFormatError(wkbType.wktType);
            }
            inPosition++;
            //2-1 write POLYGON WKB header.
            writerIndex = writeWkbHeader(outWrapper, wkbType.elementType());
            if (isEmptySet(inWrapper)) {
                inPosition = inBuffer.position();
                polygonCount++;
                continue;
            }
            inBuffer.position(inPosition);
            //2-2 write POINTS WKB.
            linearRingCount = readAndWriteLinearRing(inWrapper, outWrapper, wkbType);
            if (inBuffer.get() != ')') {
                throw createWktFormatError(wkbType.wktType);
            }
            //2-3 write point count.
            writeInt(outWrapper.outChannel, writerIndex, outWrapper.bigEndian, linearRingCount);
            inPosition = inBuffer.position();
            polygonCount++;

        }
        if (polygonCount < 1) {
            throw createWktFormatError(wkbType.wktType);
        }
        //3. write MULTI_LINE_STRING LineString count.
        writeInt(outWrapper.outChannel, elementCountWriterIndex, outWrapper.bigEndian, polygonCount);

    }


    protected static int writeWkbHeader(final WkbMemoryWrapper outWrapper, final WkbType wkbType) {
        final ByteBuf outChannel = outWrapper.outChannel;
        if (outWrapper.bigEndian) {
            outChannel.writeByte(0);
            outChannel.writeInt(wkbType.code);
        } else {
            outChannel.writeByte(1);
            outChannel.writeIntLE(wkbType.code);
        }
        final int writerIndex = outChannel.writerIndex();
        outChannel.writeZero(4);
        return writerIndex;
    }

    protected static int doGeometryHeaderToWkb(final BufferWrapper inWrapper, final WkbMemoryWrapper outWrapper
            , final WkbType wkbType) {
        //1.read wkt type.
        if (!readWktType(inWrapper, wkbType.wktType)) {
            throw createWktFormatError(wkbType.wktType);
        }
        final ByteBuffer inBuffer = inWrapper.buffer;
        if (!inBuffer.hasRemaining()) {
            throw createWktFormatError(wkbType.wktType);
        }
        final ByteBuf outChannel = outWrapper.outChannel;
        if (outWrapper.bigEndian) {
            outChannel.writeByte(0);
            outChannel.writeInt(wkbType.code);
        } else {
            outChannel.writeByte(1);
            outChannel.writeIntLE(wkbType.code);
        }
        final int writerIndex = outChannel.writerIndex();
        outChannel.writeZero(4);
        return writerIndex;
    }


    protected static byte checkByteOrder(byte byteOrder) {
        if (byteOrder != 0 && byteOrder != 1) {
            throw createIllegalByteOrderError(byteOrder);
        }
        return byteOrder;
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


    /**
     * @return count of points.
     */
    protected static int readAndWritePoints(final BufferWrapper inWrapper, final WkbOUtWrapper outWrapper
            , final WkbType wkbType) throws IllegalArgumentException {

        final int coordinates = wkbType.coordinates();
        final boolean pointText = wkbType.supportPointText(), bigEndian = outWrapper.bigEndian;

        final byte[] inArray = inWrapper.bufferArray, outArray = outWrapper.bufferArray;
        final ByteBuffer inBuffer = inWrapper.buffer, outBuffer = outWrapper.buffer;

        final int inLimit = inBuffer.limit(), outLimit = outBuffer.limit();

        int pointCount = 0, inPosition = inBuffer.position(), outPosition = outBuffer.position();
        topFor:
        for (int tempOutPosition, tempInPosition, pointEndIndex, codePoint; inPosition < inLimit; ) {
            codePoint = inArray[inPosition];
            if (Character.isWhitespace(codePoint)) {
                inPosition++;
                continue;
            }
            if (outLimit - outPosition < (coordinates << 3)) {
                // need more cumulate
                break;
            }
            if (pointText) {
                if (codePoint != '(') {
                    throw new IllegalArgumentException("Not found '(' for point text.");
                }
                inPosition++;
            }
            for (; inPosition < inLimit; inPosition++) {
                codePoint = inArray[inPosition];
                if (!Character.isWhitespace(codePoint)) {
                    break;
                }
            }
            tempInPosition = inPosition;
            tempOutPosition = outPosition;

            //parse coordinates and write to outArray.
            double d;
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
                d = Double.parseDouble(new String(Arrays.copyOfRange(inArray, startIndex, endIndex)));
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
                    pointEndIndex = tempInPosition + 1;
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
     * @return false , more read , true read success.
     */
    protected static boolean readWktType(final BufferWrapper inWrapper, final String wktType)
            throws IllegalArgumentException {

        final ByteBuffer inBuffer = inWrapper.buffer;
        final byte[] inArray = inWrapper.bufferArray;

        final int typeLength = wktType.length();
        int inLimit, inPosition, headerIndex;

        inLimit = inBuffer.limit();
        for (inPosition = inBuffer.position(); inPosition < inLimit; inPosition++) {
            if (!Character.isWhitespace(inArray[inPosition])) {
                break;
            }
        }
        if (inLimit - inPosition < typeLength) {
            inBuffer.position(inPosition);
            return false;
        }
        headerIndex = inPosition;
        for (int i = 0; i < typeLength; i++, inPosition++) {
            if (inArray[inPosition] != wktType.charAt(i)) {
                throw createWktFormatError(wktType);
            }
        }
        for (; inPosition < inLimit; inPosition++) {
            if (!Character.isWhitespace(inArray[inPosition])) {
                break;
            }
        }
        if (inLimit - inPosition < 1) {
            inBuffer.position(headerIndex);
            return false;
        }
        inBuffer.position(inPosition);
        return true;
    }


    /**
     * @param wkbType <ul>
     *                <li>POLYGON</li>
     *                <li>POLYGON Z</li>
     *                <li>POLYGON M</li>
     *                <li>POLYGON ZM</li>
     *                </ul>
     */
    protected static void doPolygonToWkb(final BufferWrapper inWrapper, final WkbMemoryWrapper outWrapper
            , final WkbType wkbType) {
        final int writerIndex = doGeometryHeaderToWkb(inWrapper, outWrapper, wkbType);
        if (isEmptySet(inWrapper)) {
            return;
        }
        if (inWrapper.buffer.get() != '(') {
            throw createWktFormatError(wkbType.wktType);
        }
        final int linearRingCount;
        linearRingCount = readAndWriteLinearRing(inWrapper, outWrapper, wkbType);
        if (linearRingCount < 1 || inWrapper.buffer.get() != ')') {
            throw createWktFormatError(wkbType.wktType);
        }
        writeInt(outWrapper.outChannel, writerIndex, outWrapper.bigEndian, linearRingCount);
    }


    /**
     * @return count of LinearRing
     * @see #readAndWritePoints(BufferWrapper, WkbOUtWrapper, WkbType)
     */
    protected static int readAndWriteLinearRing(final BufferWrapper inWrapper, final WkbMemoryWrapper outWrapper
            , final WkbType wkbType) {

        final byte[] inArray = outWrapper.bufferArray;
        final ByteBuffer inBuffer = outWrapper.buffer, outBuffer = outWrapper.buffer;
        final ByteBuf outChannel = outWrapper.outChannel;

        final byte[] startPoint = new byte[16], endPoint = new byte[startPoint.length];

        final int inLimit = inBuffer.limit();
        int codePoint, inPosition = inBuffer.position();
        int linearRingCount = 0;
        boolean linearRingEnd;
        for (int i = 0, pointWriterIndex, pointCount; inPosition < inLimit; i++) {
            codePoint = inArray[inPosition];
            if (Character.isWhitespace(codePoint)) {
                inPosition++;
                continue;
            }
            if (i > 0) {
                if (codePoint == ')') {
                    // right parenthesis. lineString end.
                    break;
                } else if (codePoint != ',') {
                    // linearRing separator
                    throw createWktFormatError(wkbType.wktType);
                }
                inPosition++;
                for (; inPosition < inLimit; inPosition++) {
                    codePoint = inArray[inPosition];
                    if (!Character.isWhitespace(codePoint)) {
                        break;
                    }
                }
            }
            if (inPosition == inLimit) {
                throw createWktFormatError(wkbType.wktType);
            }
            if (codePoint != '(') {
                // LinearRing start
                throw createWktFormatError(wkbType.wktType);
            }
            inBuffer.position(inPosition);

            pointWriterIndex = outChannel.writerIndex();
            outBuffer.putInt(0);// write placeholder of pointCount
            pointCount = 0;
            linearRingEnd = false;
            for (int j = 0, outPosition = outBuffer.position(); inPosition < inLimit; j++) {
                pointCount += readAndWritePoints(inWrapper, outWrapper, wkbType);
                if (j == 0 && pointCount > 0) {
                    outBuffer.mark();
                    outBuffer.position(outPosition);
                    outBuffer.get(startPoint);
                    outBuffer.reset();
                }
                inPosition = inBuffer.position();
                if (outBuffer.get(inPosition) == ')') {
                    inPosition++;
                    linearRingEnd = true;
                    linearRingCount++;
                }
                if (linearRingEnd && pointCount > 0) {
                    outBuffer.mark();
                    outBuffer.position(outBuffer.position() - 4);
                    outBuffer.get(endPoint);
                    outBuffer.reset();
                    if (!Arrays.equals(startPoint, endPoint)) {
                        throw createWktFormatError(wkbType.wktType);
                    }
                }
                outBuffer.flip();
                outChannel.writeBytes(outBuffer);
                outBuffer.clear();

                inBuffer.position(inPosition);
            }
            if (!linearRingEnd || pointCount < 4) {
                throw createWktFormatError(wkbType.wktType);
            }
            writeInt(outChannel, pointWriterIndex, outWrapper.bigEndian, pointCount); // write count of point.
        }
        return linearRingCount;
    }


    protected static void assertWhitespaceSuffix(BufferWrapper inWrapper, WkbType wkbType) {
        final byte[] inArray = inWrapper.bufferArray;
        for (int i = inWrapper.buffer.position(); i < inArray.length; i++) {
            if (!Character.isWhitespace(inArray[i])) {
                throw createWktFormatError(wkbType.wktType);
            }
        }
    }

    protected static boolean isEmptySet(BufferWrapper inWrapper) {
        final byte[] inArray = inWrapper.bufferArray;
        int offset = inWrapper.buffer.position();
        final boolean match;
        if (inArray.length - offset < 5) {
            match = false;
        } else {
            match = inArray[offset++] == 'E'
                    && inArray[offset++] == 'M'
                    && inArray[offset++] == 'P'
                    && inArray[offset++] == 'T'
                    && inArray[offset++] == 'Y';
        }
        if (match) {
            inWrapper.buffer.position(offset);
        }
        return match;
    }

    protected static String createEmptySet(WkbType wkbType) {
        return wkbType.wktType + " EMPTY";
    }

    protected static void assertPoint(WkbType wkbType) {
        if (!isPoint(wkbType)) {
            throw createUnsupportedWkb(wkbType);
        }
    }

    protected static boolean isPoint(final WkbType wkbType) {
        final boolean match;
        switch (wkbType) {
            case POINT:
            case POINT_Z:
            case POINT_M:
            case POINT_ZM:
                match = true;
                break;
            default:
                match = false;
        }
        return match;
    }

    protected static boolean isCollection(final WkbType wkbType) {
        final boolean match;
        switch (wkbType) {
            case GEOMETRY_COLLECTION:
            case GEOMETRY_COLLECTION_Z:
            case GEOMETRY_COLLECTION_M:
            case GEOMETRY_COLLECTION_ZM:
                match = true;
                break;
            default:
                match = false;
        }
        return match;
    }

    protected static void skipWhitespace(final BufferWrapper inWrapper) {
        final byte[] inArray = inWrapper.bufferArray;
        final ByteBuffer inBuffer = inWrapper.buffer;
        final int inLimit = inBuffer.limit();

        int inPosition = inBuffer.position();
        for (; inPosition < inLimit; inPosition++) {
            if (!Character.isWhitespace(inArray[inPosition])) {
                break;
            }
        }
        inBuffer.position(inPosition);
    }

    protected static IllegalArgumentException createIllegalByteOrderError(byte byteOrder) {
        return new IllegalArgumentException(String.format("Illegal byteOrder[%s].", byteOrder));
    }

    protected static IllegalArgumentException createIllegalWkbLengthError(long length, long expectLength) {
        return new IllegalArgumentException(String.format("WKB length[%s] but expect min length[%s]."
                , length, expectLength));
    }


    protected static IllegalArgumentException createWkbLengthError(String type, long length, long exceptLength) {
        return new IllegalArgumentException(String.format("WKB length[%s] and %s except length[%s] not match."
                , length, type, exceptLength));
    }

    protected static IllegalArgumentException createUnsupportedWkb(WkbType wkbType) {
        return new IllegalArgumentException(String.format("Unsupported WKB[%s]", wkbType));
    }


    protected static IllegalArgumentException createOffsetError(int offset, int arrayLength) {
        return new IllegalArgumentException(String.format("offset[%s] not in [0,%s).", offset, arrayLength));
    }

    protected static IllegalArgumentException createIllegalElementCount(int elementCount) {
        return new IllegalArgumentException(String.format("elementCount[%s] error", elementCount));
    }


    protected static IllegalArgumentException createIllegalLinearPointCountError(int pointCount) {
        return new IllegalArgumentException(String.format("LinearRing pointCount[%s] error", pointCount));
    }


    protected static IllegalArgumentException createNonLinearRingError(int linearRingIndex) {
        return new IllegalArgumentException(String.format("Polygon LinearRing[%s] isn't LinearRing", linearRingIndex));
    }

    protected static IllegalArgumentException createNonDoubleError(String nonDouble) {
        return new IllegalArgumentException(String.format("%s isn't double number.", nonDouble));
    }


    protected static IllegalArgumentException createWktFormatError(String wktType) {
        return new IllegalArgumentException(String.format("Not %s WKT format.", wktType));
    }

    protected static IllegalArgumentException createNonLinearRingError() {
        return new IllegalArgumentException("Found not close LinearRing.");
    }


    protected static class WkbOUtWrapper extends BufferWrapper {

        protected final boolean bigEndian;


        protected WkbOUtWrapper(byte[] bufferArray, boolean bigEndian) {
            super(bufferArray);
            this.bigEndian = bigEndian;
        }

        protected WkbOUtWrapper(int arrayLength, boolean bigEndian) {
            super(arrayLength);
            this.bigEndian = bigEndian;
        }

    }

    protected static class WkbMemoryWrapper extends WkbOUtWrapper {

        protected final ByteBuf outChannel;

        protected WkbMemoryWrapper(byte[] bufferArray, boolean bigEndian) {
            super(bufferArray, bigEndian);
            this.outChannel = ByteBufAllocator.DEFAULT.buffer(bufferArray.length, (1 << 30));
        }

        protected WkbMemoryWrapper(int arrayLength, boolean bigEndian) {
            super(arrayLength, bigEndian);
            this.outChannel = ByteBufAllocator.DEFAULT.buffer(arrayLength, (1 << 30));
        }

    }




    /*################################## blow private method ##################################*/

    /**
     * @see #wkbEquals(byte[], byte[])
     */
    private static boolean geometryReverseEquals(final byte[] geometryOne, final byte[] geometryTwo
            , final WkbType wkbType) {
        int offset = 5;
        if (!JdbdArrayUtils.reverseEquals(geometryOne, geometryTwo, offset, 4, 1)) {
            return false;
        }
        final int elementCount;
        elementCount = JdbdNumberUtils.readIntFromEndian(geometryOne[0] == 0, geometryOne, offset, 4);
        if (elementCount == 0) {
            return true;
        }
        offset += 4;
        final boolean match;
        switch (wkbType) {
            case MULTI_POINT:
            case MULTI_POINT_Z:
            case MULTI_POINT_M:
            case MULTI_POINT_ZM:
                match = multiPointEquals(geometryOne, geometryTwo, elementCount, offset) > 0;
                break;
            case LINE_STRING:
            case LINE_STRING_Z:
            case LINE_STRING_M:
            case LINE_STRING_ZM:
                match = JdbdArrayUtils.reverseEquals(geometryOne, geometryTwo, offset, 8, wkbType.coordinates());
                break;
            case POLYGON:
                match = polygonReverseEquals(geometryOne, geometryTwo, elementCount, offset) > 0;
                break;
            case MULTI_LINE_STRING:
                match = multiLineStringReverseEquals(geometryOne, geometryTwo, elementCount, offset) > 0;
                break;
            case MULTI_POLYGON:
                match = multiPolygonReverseEquals(geometryOne, geometryTwo, elementCount, offset) > 0;
                break;
            case GEOMETRY_COLLECTION:
                match = geometryCollectionReverseEquals(geometryOne, geometryTwo) > 0;
                break;
            default:
                throw new IllegalArgumentException(String.format("not support %s now.", wkbType));
        }
        return match;
    }

    /**
     * @see #wkbEquals(byte[], byte[])
     */
    private static int geometryCollectionReverseEquals(final byte[] geometryOne, final byte[] geometryTwo) {
        boolean bigEndianOne, bigEndianTwo;
        WkbType wkbTypeOne, wkbTypeTwo;
        final Stack<Pair<Integer, Integer>> geometryCountStack = new Stack<>();
        Pair<Integer, Integer> pair;
        int offset = 0;
        topFor:
        for (int i = 0, geometryCount = 1, elementCount; i < geometryCount; ) {
            wkbTypeOne = WkbType.resolveWkbType(geometryOne, offset);
            wkbTypeTwo = WkbType.resolveWkbType(geometryTwo, offset);
            if (wkbTypeOne != wkbTypeTwo) {
                offset = -1;
                break;
            }
            bigEndianOne = geometryOne[offset] == 0;
            bigEndianTwo = geometryTwo[offset] == 0;
            offset += 5;
            if (isPoint(wkbTypeOne)) {
                if (bigEndianOne == bigEndianTwo) {
                    if (!JdbdArrayUtils.equals(geometryOne, geometryTwo, offset, wkbTypeOne.coordinates() << 3)) {
                        offset = -1;
                        break;
                    }
                } else if (!JdbdArrayUtils.reverseEquals(geometryOne, geometryTwo, offset, 8
                        , wkbTypeOne.coordinates())) {
                    offset = -1;
                    break;
                }
                i++;
                continue;
            } else if ((geometryOne.length - offset) < 4) {
                throw createWkbLengthError(wkbTypeOne.name(), geometryOne.length - offset, offset + HEADER_LENGTH);
            }
            if (!JdbdArrayUtils.reverseEquals(geometryOne, geometryTwo, offset, 4, 1)) {
                offset = -1;
                break;
            }
            elementCount = JdbdNumberUtils.readIntFromEndian(bigEndianOne, geometryOne, offset, 4);
            offset += 4;
            if (elementCount < 0) {
                throw new IllegalArgumentException("WKB format error,elementCount less than 0 .");
            } else if (elementCount == 0) {
                if (offset != geometryOne.length) {
                    throw new IllegalArgumentException("WKB format error.");
                }
                return offset;
            }
            switch (wkbTypeOne) {
                case MULTI_POINT:
                case LINE_STRING: {
                    if (bigEndianOne == bigEndianTwo) {
                        if (!JdbdArrayUtils.equals(geometryOne, geometryTwo, offset, wkbTypeOne.coordinates() << 3)) {
                            offset = -1;
                            break topFor;
                        }
                    } else if (!JdbdArrayUtils.reverseEquals(geometryOne, geometryTwo, offset, 8, elementCount << 1)) {
                        offset = -1;
                        break topFor;
                    }
                    offset += (elementCount << 4);
                }
                break;
                case POLYGON:
                    offset = polygonReverseEquals(geometryOne, geometryTwo, elementCount, offset);
                    break;
                case MULTI_LINE_STRING:
                    offset = multiLineStringReverseEquals(geometryOne, geometryTwo, elementCount, offset);
                    break;
                case MULTI_POLYGON:
                    offset = multiPolygonReverseEquals(geometryOne, geometryTwo, elementCount, offset);
                    break;
                case GEOMETRY_COLLECTION:
                    geometryCountStack.push(new Pair<>(geometryCount, i + 1));
                    geometryCount = elementCount;
                    i = 0;
                    continue;
                default:
                    throw new IllegalArgumentException(String.format("not support %s now.", wkbTypeOne));
            }
            i++;
            if (i == geometryCount) {
                if (geometryCountStack.isEmpty()) {
                    break;
                }
                pair = geometryCountStack.pop();
                geometryCount = pair.getFirst();
                i = pair.getSecond();
            }
        }
        return offset;
    }


    /**
     * @see #geometryCollectionReverseEquals(byte[], byte[])
     */
    private static int polygonReverseEquals(final byte[] polygonOne, final byte[] polygonTwo
            , final int elementCount, int offset) {
        final boolean bigEndian = polygonOne[0] == 0;
        for (int i = 0, pointCount; i < elementCount; i++) {
            if (!JdbdArrayUtils.reverseEquals(polygonOne, polygonTwo, offset, 4, 1)) {
                offset = -1;
                break;
            }
            pointCount = JdbdNumberUtils.readIntFromEndian(bigEndian, polygonOne, offset, 4);
            if (pointCount < 4) {
                throw createIllegalLinearPointCountError(pointCount);
            }
            offset += 4;
            if (!JdbdArrayUtils.reverseEquals(polygonOne, polygonTwo, offset, 8, pointCount << 1)) {
                offset = -1;
                break;
            }
            offset += (pointCount << 4);
        }
        return offset;
    }

    /**
     * @see #geometryReverseEquals(byte[], byte[], WkbType)
     * @see #geometryCollectionReverseEquals(byte[], byte[])
     */
    private static int multiPointEquals(final byte[] multiPointOne, final byte[] multiPointTwo, final int elementCount
            , int offset) {
        WkbType wkbTypeOne, wkbTypeTwo;
        boolean sameEndian;
        for (int i = 0, coordinates, numBytes; i < elementCount; i++) {
            wkbTypeOne = WkbType.resolveWkbType(multiPointOne, offset);
            wkbTypeTwo = WkbType.resolveWkbType(multiPointTwo, offset);
            if (wkbTypeOne != wkbTypeTwo) {
                offset = -1;
                break;
            }
            if (!isPoint(wkbTypeOne)) {
                throw new IllegalArgumentException("Non MultiPoint WKB.");
            }
            sameEndian = multiPointOne[offset] == multiPointTwo[offset];
            offset += 5;
            coordinates = wkbTypeOne.coordinates();
            numBytes = coordinates << 3;

            if (multiPointOne.length < offset + numBytes) {
                throw createWkbLengthError(wkbTypeOne.wktType, multiPointOne.length, offset + numBytes);
            }
            if (sameEndian) {
                if (!JdbdArrayUtils.equals(multiPointOne, multiPointTwo, offset, numBytes)) {
                    offset = -1;
                    break;
                }
            } else if (!JdbdArrayUtils.reverseEquals(multiPointOne, multiPointTwo, offset, 8, coordinates)) {
                offset = -1;
                break;
            }

            offset += numBytes;
        }
        return offset;
    }

    /**
     * @see #geometryCollectionReverseEquals(byte[], byte[])
     */
    private static int multiLineStringReverseEquals(final byte[] multiLineStringOne,
                                                    final byte[] multiLineStringTwo
            , final int elementCount, int offset) {
        boolean bigEndian;
        WkbType wkbTypeOne, wkbTypeTwo;
        for (int i = 0, pointCount; i < elementCount; i++) {
            wkbTypeOne = WkbType.resolveWkbType(multiLineStringOne, offset);
            wkbTypeTwo = WkbType.resolveWkbType(multiLineStringTwo, offset);
            if (wkbTypeOne != wkbTypeTwo) {
                offset = -1;
                break;
            }
            if (wkbTypeOne != WkbType.LINE_STRING) {
                throw new IllegalArgumentException("Error WKB non-linestring element type.");
            }
            bigEndian = multiLineStringOne[offset] == 0;
            offset += 5;
            if (!JdbdArrayUtils.reverseEquals(multiLineStringOne, multiLineStringTwo, offset, 4, 1)) {
                offset = -1;
                break;
            }
            pointCount = JdbdNumberUtils.readIntFromEndian(bigEndian, multiLineStringOne, offset, 4);
            if (pointCount < 4) {
                throw createIllegalLinearPointCountError(pointCount);
            }
            offset += 4;
            if (!JdbdArrayUtils.reverseEquals(multiLineStringOne, multiLineStringTwo, offset, 8, pointCount << 1)) {
                offset = -1;
                break;
            }
            offset += (pointCount << 4);
        }
        return offset;
    }

    /**
     * @see #geometryCollectionReverseEquals(byte[], byte[])
     */
    private static int multiPolygonReverseEquals(final byte[] multiPolygonOne, final byte[] multiPolygonTwo
            , final int elementCount, int offset) {
        boolean bigEndian;
        WkbType wkbTypeOne, wkbTypeTwo;
        for (int i = 0, pointCount; i < elementCount; i++) {
            wkbTypeOne = WkbType.resolveWkbType(multiPolygonOne, offset);
            wkbTypeTwo = WkbType.resolveWkbType(multiPolygonTwo, offset);
            if (wkbTypeOne != wkbTypeTwo) {
                offset = -1;
                break;
            }
            if (wkbTypeOne != WkbType.POLYGON) {
                throw new IllegalArgumentException("Error WKB non-polygon element type.");
            }
            bigEndian = checkByteOrder(multiPolygonOne[offset]) == 0;
            offset += 5;
            if (!JdbdArrayUtils.reverseEquals(multiPolygonOne, multiPolygonTwo, offset, 4, 1)) {
                offset = -1;
                break;
            }
            pointCount = JdbdNumberUtils.readIntFromEndian(bigEndian, multiPolygonOne, offset, 4);
            if (pointCount < 4) {
                throw createIllegalLinearPointCountError(pointCount);
            }
            offset += 4;
            if (!JdbdArrayUtils.reverseEquals(multiPolygonOne, multiPolygonTwo, offset, 8, pointCount << 1)) {
                offset = -1;
                break;
            }
            offset += (pointCount << 4);
        }
        return offset;

    }


}
