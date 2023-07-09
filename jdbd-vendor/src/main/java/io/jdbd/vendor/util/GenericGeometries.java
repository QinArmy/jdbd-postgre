package io.jdbd.vendor.util;

import io.jdbd.type.geometry.WkbType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

@Deprecated
abstract class GenericGeometries {

    GenericGeometries() {
        throw new UnsupportedOperationException();
    }

    private static final Logger LOG = LoggerFactory.getLogger(GenericGeometries.class);

    static final byte HEADER_LENGTH = 9;


    public static boolean readEndian(final byte endianByte) {
        final boolean endian;
        switch (endianByte) {
            case 0:
                endian = true;
                break;
            case 1:
                endian = false;
                break;
            default:
                throw new IllegalArgumentException(String.format("endianByte[%s] error.", endianByte));
        }
        return endian;
    }

    /**
     * <p>
     * Check LINESTRING wkb and return point count.
     * </p>
     *
     * @param wkb inclusive point
     * @return element count
     */
    public static int checkLineStringWkb(byte[] wkb, WkbType expect) {
        if (wkb.length < 9) {
            throw new IllegalArgumentException("wkb length less than 9");
        } else if (expect.family() != WkbType.LINE_STRING) {
            throw new IllegalArgumentException("expect error");
        }

        final boolean endian = readEndian(wkb[0]);
        if (WkbType.fromWkbArray(wkb, 0) != expect) {
            throw new IllegalArgumentException(String.format("wkb type isn't expect type[%s]", expect));
        }
        final int elementCount = JdbdNumbers.readIntFromEndian(endian, wkb, 5, 4);
        if (wkb.length != (9 + (elementCount * 8 * expect.coordinates()))) {
            throw new IllegalArgumentException("wkb length error.");
        }
        return elementCount;
    }

    public static boolean wkbEquals(final byte[] geometryOne, final byte[] geometryTwo) {
        final boolean match;
        if (geometryOne.length != geometryTwo.length) {
            match = false;
        } else {
            int offset = geometryEquals(geometryOne, geometryTwo, 0);
            if (offset < 0) {
                match = false;
            } else if (offset == geometryOne.length) {
                match = true;
            } else {
                throw new IllegalArgumentException("WKB length error.");
            }
        }
        return match;
    }

    public static int readElementCount(final byte[] wkbArray) {
        WkbType wkbType = WkbType.fromWkbArray(wkbArray, 0);
        final int elementCount;
        if (wkbType.family() == WkbType.POINT) {
            if (wkbArray.length == 5) {
                elementCount = 0;
            } else if (wkbArray.length == (5 + (wkbType.coordinates() << 3))) {
                elementCount = 1;
            } else {
                throw createIllegalWkbLengthError(wkbArray.length, (5 + ((long) wkbType.coordinates() << 3)));
            }
        } else {
            if (wkbArray.length < HEADER_LENGTH) {
                throw createIllegalWkbLengthError(wkbArray.length, HEADER_LENGTH);
            } else {
                elementCount = JdbdNumbers.readIntFromEndian(wkbArray[0] == 0, wkbArray, 5, 4);
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
        final WkbType wkbType = WkbType.fromWkbArray(wkbArray, offset);
        if (wkbType.family() != WkbType.POINT) {
            throw createUnsupportedWkb(wkbType);
        }
        if (wkbArray.length - offset == 5) {
            return createEmptySet(wkbType);
        }
        StringBuilder builder = new StringBuilder(wkbType.wktType.length() + wkbType.coordinates() * 10)
                .append(wkbType.wktType);
        pointTextToWkt(builder, wkbArray, wkbType, offset);
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
        final int offset = offsetOut[0];
        if (offset < 0 || offset >= wkbArray.length) {
            throw createOffsetError(offset, wkbArray.length);
        }
        final WkbType wkbType = WkbType.fromWkbArray(wkbArray, offset);

        if (wkbType.family() != WkbType.LINE_STRING) {
            throw new IllegalArgumentException(String.format("Unsupported WKB[%s] type.", wkbType));
        }
        if (wkbArray.length - offset < HEADER_LENGTH) {
            throw createWkbLengthError(wkbType.wktType, wkbArray.length, offset + HEADER_LENGTH);
        }

        final boolean bigEndian = wkbArray[offset] == 0;
        final int pointCount = JdbdNumbers.readIntFromEndian(bigEndian, wkbArray, offset + 5, 4);
        StringBuilder builder = new StringBuilder(wkbType.coordinates() * pointCount * 8)
                .append(wkbType.wktType);
        offsetOut[0] = lineStringTextToWkt(builder, wkbArray, wkbType, offset);
        return builder.toString();
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
        final int offset = offsetOut[0];
        final WkbType wkbType = WkbType.fromWkbArray(wkbArray, offset);
        if (wkbType != WkbType.POLYGON) {
            throw createUnsupportedWkb(wkbType);
        }

        final boolean bigEndian = wkbArray[offset] == 0;

        final int lineStringCount;
        lineStringCount = JdbdNumbers.readIntFromEndian(bigEndian, wkbArray, offset + 5, 4);

        StringBuilder builder = new StringBuilder(wkbType.coordinates() * 8 * 4 * lineStringCount)
                .append(wkbType.wktType);

        offsetOut[0] = polygonTextToWkt(builder, wkbType, wkbArray, offset);
        return builder.toString();
    }

    /**
     * @see #multiPointToWkt(byte[], int[])
     */
    public static String multiPointToWkt(final byte[] wkbArray) {
        return multiPointToWkt(wkbArray, new int[]{0});
    }

    /**
     * @param wkbArray  <ul>
     *                  <li>{@link WkbType#MULTI_POINT}</li>
     *                  <li>{@link WkbType#MULTI_POINT_Z}</li>
     *                  <li>{@link WkbType#MULTI_POINT_M}</li>
     *                  <li>{@link WkbType#MULTI_POINT_ZM}</li>
     *                  </ul>
     * @param offsetOut array than length is 1.
     */
    public static String multiPointToWkt(final byte[] wkbArray, final int[] offsetOut) {
        final int offset = offsetOut[0];
        if (offset < 0 || offset >= wkbArray.length) {
            throw createOffsetError(offset, wkbArray.length);
        }
        final WkbType wkbType = WkbType.fromWkbArray(wkbArray, offset);

        if (wkbType.family() != WkbType.MULTI_POINT) {
            throw createUnsupportedWkb(wkbType);
        }
        if (wkbArray.length < offset + HEADER_LENGTH) {
            throw createWkbLengthError(wkbType.wktType, wkbArray.length, offset + HEADER_LENGTH);
        }
        final int pointCount;
        pointCount = JdbdNumbers.readIntFromEndian(wkbArray[offset] == 0, wkbArray, offset + 5, 4);
        StringBuilder builder = new StringBuilder((wkbType.coordinates() << 3) * pointCount)
                .append(wkbType.wktType);
        offsetOut[0] = multiPointTextToWkt(builder, wkbArray, wkbType, offset);
        return builder.toString();
    }

    public static String multiLineStringToWkt(final byte[] wkbArray) {
        return multiLineStringToWkt(wkbArray, new int[]{0});
    }

    /**
     * @param wkbArray  <ul>
     *                  <li>{@link WkbType#MULTI_LINE_STRING}</li>
     *                  <li>{@link WkbType#MULTI_LINE_STRING_Z}</li>
     *                  <li>{@link WkbType#MULTI_LINE_STRING_M}</li>
     *                  <li>{@link WkbType#MULTI_LINE_STRING_ZM}</li>
     *                  </ul>
     * @param offsetOut array than length is 1.
     */
    public static String multiLineStringToWkt(final byte[] wkbArray, final int[] offsetOut) {
        final int offset = offsetOut[0];
        if (offset < 0 || offset >= wkbArray.length) {
            throw createOffsetError(offset, wkbArray.length);
        }
        final WkbType wkbType = WkbType.fromWkbArray(wkbArray, offset);

        if (wkbType.family() != WkbType.MULTI_LINE_STRING) {
            throw createUnsupportedWkb(wkbType);
        }
        if (wkbArray.length - offset < HEADER_LENGTH) {
            throw createWkbLengthError(wkbType.wktType, wkbArray.length, offset + HEADER_LENGTH);
        }
        boolean bigEndian = wkbArray[offset] == 0;
        final int lineStringCount = JdbdNumbers.readIntFromEndian(bigEndian, wkbArray, offset + 5, 4);
        StringBuilder builder = new StringBuilder((wkbType.coordinates() << 3) * lineStringCount)
                .append(wkbType.wktType);

        offsetOut[0] = multiLineStringTextToWkt(builder, wkbArray, wkbType, offset);
        return builder.toString();
    }

    public static String multiPolygonToWkt(final byte[] wkbArray) {
        return multiPolygonToWkt(wkbArray, new int[]{0});
    }

    public static String multiPolygonToWkt(final byte[] wkbArray, final int[] offsetOut) {
        final int offset = offsetOut[0];
        if (offset < 0 || offset >= wkbArray.length) {
            throw createOffsetError(offset, wkbArray.length);
        }
        final WkbType wkbType = WkbType.fromWkbArray(wkbArray, offset);

        if (wkbType.family() != WkbType.MULTI_POLYGON) {
            throw createUnsupportedWkb(wkbType);
        }
        if (wkbArray.length < offset + HEADER_LENGTH) {
            throw createWkbLengthError(wkbType.wktType, wkbArray.length, offset + HEADER_LENGTH);
        }
        final boolean bigEndian = wkbArray[offset] == 0;
        final int polygonCount = JdbdNumbers.readIntFromEndian(bigEndian, wkbArray, offset + 5, 4);
        StringBuilder builder = new StringBuilder((wkbType.coordinates() << 3) * polygonCount)
                .append(wkbType.wktType);
        offsetOut[0] = multiPolygonTextToWkt(builder, wkbArray, wkbType, offset);
        return builder.toString();
    }

    public static String geometryCollectionToWkt(final byte[] wkbArray) {
        return geometryCollectionToWkt(wkbArray, new int[]{0});
    }

    /**
     * @param wkbArray <ul>
     *                 <li>{@link WkbType#GEOMETRY_COLLECTION}</li>
     *                 <li>{@link WkbType#GEOMETRY_COLLECTION_Z}</li>
     *                 <li>{@link WkbType#GEOMETRY_COLLECTION_M}</li>
     *                 <li>{@link WkbType#GEOMETRY_COLLECTION_ZM}</li>
     *                 </ul>
     */
    public static String geometryCollectionToWkt(final byte[] wkbArray, final int[] offsetOut) {
        final int offset = offsetOut[0];
        if (offset < 0 || offset >= wkbArray.length) {
            throw createOffsetError(offset, wkbArray.length);
        }
        if (wkbArray.length < offset + HEADER_LENGTH) {
            throw createIllegalWkbLengthError(wkbArray.length, offset + HEADER_LENGTH);
        }
        final WkbType wkbType = WkbType.fromWkbArray(wkbArray, offset);
        if (wkbType.family() != WkbType.GEOMETRY_COLLECTION) {
            throw createUnsupportedWkb(wkbType);
        }
        final boolean bigEndian = wkbArray[offset] == 0;
        final int geometryCount = JdbdNumbers.readIntFromEndian(bigEndian, wkbArray, offset + 5, 4);

        StringBuilder builder = new StringBuilder(geometryCount * 10)
                .append(wkbType.wktType);

        offsetOut[0] = geometryCollectionTextToWkt(builder, wkbArray, wkbType, offset);
        return builder.toString();
    }


    /*################################## blow package method ##################################*/

    /**
     * @param inBuffer must be created by {@link ByteBuffer#wrap(byte[])} method.
     * @param wkbType  <ul>
     *                 <li>{@link WkbType#POINT}</li>
     *                 <li>{@link WkbType#POINT_Z}</li>
     *                 <li>{@link WkbType#POINT_M}</li>
     *                 <li>{@link WkbType#POINT_ZM}</li>
     *                 </ul>
     */
    static int pointTextToWkb(final ByteBuffer inBuffer, final WkbOUtWrapper outWrapper
            , final WkbType wkbType) {
        if (wkbType.family() != WkbType.POINT) {
            throw createUnsupportedWkb(wkbType);
        }
        skipWhitespace(inBuffer);
        if (isEmptySet(inBuffer)) {
            return 0;
        }
        if (inBuffer.get() != '(') {
            throw createWktFormatError(wkbType.wktType);
        }
        if (readAndWritePoints(inBuffer, outWrapper, wkbType) != 1) {
            throw createWktFormatError(wkbType.wktType);
        }
        if (inBuffer.get() != ')') {
            throw new IllegalArgumentException("Point text not close.");
        }
        return 1;
    }


    /**
     * @param inBuffer must be created by {@link ByteBuffer#wrap(byte[])} method.
     * @return point count.
     */
    static int lineStringTextToWkb(final ByteBuffer inBuffer, final WkbMemoryWrapper outWrapper
            , final WkbType wkbType) {

        if (wkbType.family() != WkbType.LINE_STRING) {
            throw createUnsupportedWkb(wkbType);
        }
        if (prepareGeometryText(inBuffer, outWrapper, wkbType)) {
            return 0;
        }
        final ByteBuffer outBuffer = outWrapper.buffer;
        final ByteBuf outChannel = outWrapper.outChannel;
        final int inLimit = inBuffer.limit();

        int pointCount = 0;
        boolean lineStringEnd = false;
        for (int inPosition = inBuffer.position(), outPosition; inPosition < inLimit; ) {
            outPosition = outBuffer.position();
            pointCount += readAndWritePoints(inBuffer, outWrapper, wkbType);
            if (outBuffer.position() == outPosition) {
                throw createWktFormatError(wkbType.wktType);
            }
            outBuffer.flip();
            outChannel.writeBytes(outBuffer);
            outBuffer.clear();

            inPosition = inBuffer.position();
            if (inBuffer.get(inPosition) == ')') {
                lineStringEnd = true;
                inBuffer.get(); //skip
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
     * @return new offset
     * @see #pointToWkt(byte[], int[])
     * @see #geometryCollectionTextToWkt(StringBuilder, byte[], WkbType, int)
     */
    static int pointTextToWkt(final StringBuilder builder, final byte[] wkbArray, final WkbType wkbType, int offset) {
        if (wkbType != WkbType.POINT) {
            throw createUnsupportedWkb(wkbType);
        }
        final boolean bigEndian = wkbArray[offset] == 0;
        offset += 5;
        final int coordinates = wkbType.coordinates(), coordinateBytes = coordinates << 3;
        if (wkbArray.length == offset) {
            builder.append(" EMPTY");
        } else if (wkbArray.length >= offset + coordinateBytes) {
            builder.append("(");
            for (int i = 0; i < coordinates; i++) {
                if (i > 0) {
                    builder.append(" ");
                }
                builder.append(JdbdNumbers.readDoubleFromEndian(bigEndian, wkbArray, offset, 8));
                offset += 8;
            }
            builder.append(")");
        }
        return offset;
    }

    /**
     * @return new offset.
     * @see #polygonToWkt(byte[], int[])
     * @see #multiPolygonToWkt(byte[], int[])
     * @see #geometryCollectionTextToWkt(StringBuilder, byte[], WkbType, int)
     */
    static int polygonTextToWkt(final StringBuilder builder, final WkbType wkbType, final byte[] wkbArray
            , int offset) {
        if (offset < 0 || offset >= wkbArray.length) {
            throw createOffsetError(offset, wkbArray.length);
        }
        if (wkbType != WkbType.POLYGON) {
            throw createUnsupportedWkb(wkbType);
        }
        if (wkbArray.length - offset < HEADER_LENGTH) {
            throw createWkbLengthError(wkbType.wktType, wkbArray.length, offset + HEADER_LENGTH);
        }
        final boolean bigEndian = wkbArray[offset] == 0;
        offset += 5;
        final int lineStringCount;
        lineStringCount = JdbdNumbers.readIntFromEndian(bigEndian, wkbArray, offset, 4);
        offset += 4;
        if (lineStringCount < 0) {
            throw createIllegalElementCount(lineStringCount);
        } else if (lineStringCount == 0) {
            builder.append(" EMPTY");
            return offset;
        }

        final int coordinates = wkbType.coordinates();
        builder.append("(");

        final byte[] startPointArray = new byte[coordinates << 3], endPointArray = new byte[startPointArray.length];
        // output polygon
        for (int i = 0, pointCount; i < lineStringCount; i++) {
            if (i > 0) {
                builder.append(",");
            }
            pointCount = JdbdNumbers.readIntFromEndian(bigEndian, wkbArray, offset, 4);
            if (pointCount < 4) {
                throw createIllegalLinearPointCountError(pointCount);
            }
            offset += 4;
            if (wkbArray.length - offset < pointCount * startPointArray.length) {
                throw createWkbLengthError(wkbType.wktType, wkbArray.length
                        , offset + (long) pointCount * startPointArray.length);
            }
            // output one LinearRing text
            builder.append("(");
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
                    builder.append(JdbdNumbers.readDoubleFromEndian(bigEndian, wkbArray, offset, 8));
                    offset += 8;
                }
            }
            builder.append(")");
        }
        builder.append(")");
        return offset;
    }


    /**
     * @param inBuffer must be created by {@link ByteBuffer#wrap(byte[])} method.
     * @param wkbType  <ul>
     *                 <li>MultiPoint</li>
     *                 <li>MultiPoint Z</li>
     *                 <li>MultiPoint M</li>
     *                 <li>MultiPoint ZM</li>
     *                 </ul>
     */
    static int multiPointTextToWkb(final ByteBuffer inBuffer, final WkbMemoryWrapper outWrapper
            , final WkbType wkbType) {
        if (wkbType.family() != WkbType.MULTI_POINT) {
            throw createUnsupportedWkb(wkbType);
        }
        if (prepareGeometryText(inBuffer, outWrapper, wkbType)) {
            return 0;
        }
        final ByteBuffer outBuffer = outWrapper.buffer;
        final ByteBuf outChannel = outWrapper.outChannel;

        final int inLimit = inBuffer.limit();
        final byte[] pointArray = new byte[wkbType.coordinates() << 3];

        int pointCount = 0;
        final WkbType elementType = Objects.requireNonNull(wkbType.elementType());
        //2. read point
        boolean multiPointEnd = false;
        for (int inPosition = inBuffer.position(), outPosition, pointNum; inPosition < inLimit; ) {
            outPosition = outBuffer.position();
            pointNum = readAndWritePoints(inBuffer, outWrapper, wkbType);
            if (outBuffer.position() == outPosition) {
                throw createWktFormatError(wkbType.wktType);
            }
            pointCount += pointNum;
            outBuffer.flip();
            for (int j = 0; j < pointNum; j++) {
                if (outWrapper.bigEndian) {
                    outChannel.writeByte(0);
                    outChannel.writeInt(elementType.code);
                } else {
                    outChannel.writeByte(1);
                    outChannel.writeIntLE(elementType.code);
                }
                outBuffer.get(pointArray);
                outChannel.writeBytes(pointArray);
            }
            outBuffer.clear();
            inPosition = inBuffer.position();
            if (inBuffer.get(inPosition) == ')') {
                inBuffer.get(); //skip right parenthesis.
                multiPointEnd = true;
                break;
            }
        }
        if (!multiPointEnd) {
            throw new IllegalArgumentException(String.format("%s not close.", wkbType));
        }

        return pointCount;
    }

    /**
     * @param inBuffer must be created by {@link ByteBuffer#wrap(byte[])} method.
     * @param wkbType  <ul>
     *                 <li>{@link WkbType#MULTI_LINE_STRING}</li>
     *                 <li>{@link WkbType#MULTI_LINE_STRING_Z}</li>
     *                 <li>{@link WkbType#MULTI_LINE_STRING_M}</li>
     *                 <li>{@link WkbType#MULTI_LINE_STRING_ZM}</li>
     *                 </ul>
     * @return LINE_STRING count.
     */
    static int multiLineStringTextToWkb(final ByteBuffer inBuffer, final WkbMemoryWrapper outWrapper
            , final WkbType wkbType) {
        if (wkbType.family() != WkbType.MULTI_LINE_STRING) {
            throw createUnsupportedWkb(wkbType);
        }
        if (prepareGeometryText(inBuffer, outWrapper, wkbType)) {
            return 0;
        }
        int lineStringCount = 0, inPosition = inBuffer.position();
        final WkbType elementType = Objects.requireNonNull(wkbType.elementType());
        final byte[] inArray = inBuffer.array();
        final int inLimit = inBuffer.limit();
        //2. write LineString wkb
        boolean multiLineStringEnd = false;
        for (int codePoint, writerIndex, pointCount; inPosition < inLimit; ) {
            codePoint = inArray[inPosition];
            if (Character.isWhitespace(codePoint)) {
                inPosition++;
                continue;
            }
            if (lineStringCount > 0) {
                if (codePoint == ')') {
                    // right parenthesis,MultiLineString end.
                    inBuffer.position(inPosition + 1);
                    multiLineStringEnd = true;
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
            inBuffer.position(inPosition);
            //2-1 write LineString WKB header.
            writerIndex = writeGeometryHeader(outWrapper, elementType);
            //2-2 write LineString text WKB.
            pointCount = lineStringTextToWkb(inBuffer, outWrapper, elementType);
            //2-3 write point count.
            if (pointCount != 0) {
                writeInt(outWrapper.outChannel, writerIndex, outWrapper.bigEndian, pointCount);
            }
            inPosition = inBuffer.position();
            lineStringCount++;

        }
        if (!multiLineStringEnd) {
            throw new IllegalArgumentException(String.format("%s not close.", wkbType));
        }
        return lineStringCount;
    }


    /**
     * @param inBuffer must be created by {@link ByteBuffer#wrap(byte[])} method.
     * @param wkbType  <ul>
     *                 <li>{@link WkbType#MULTI_POLYGON}</li>
     *                 <li>{@link WkbType#MULTI_POLYGON_Z}</li>
     *                 <li>{@link WkbType#MULTI_POLYGON_M}</li>
     *                 <li>{@link WkbType#MULTI_POLYGON_ZM}</li>
     *                 </ul>
     * @return polygon count
     */
    static int multiPolygonTextToWkb(final ByteBuffer inBuffer, final WkbMemoryWrapper outWrapper
            , final WkbType wkbType) {
        if (wkbType.family() != WkbType.MULTI_POLYGON) {
            throw new IllegalArgumentException(String.format("Unsupported WKB[%s]", wkbType));
        }
        if (prepareGeometryText(inBuffer, outWrapper, wkbType)) {
            return 0;
        }
        final ByteBuf outChannel = outWrapper.outChannel;

        final WkbType elementType = Objects.requireNonNull(wkbType.elementType());
        int polygonCount = 0;
        final int inLimit = inBuffer.limit();
        final byte[] inArray = inBuffer.array();
        //2. write MULTI_POLYGON wkb
        boolean multiPolygonEnd = false;
        for (int codePoint, writerIndex, linearRingCount, inPosition = inBuffer.position(); inPosition < inLimit; ) {
            codePoint = inArray[inPosition];
            if (Character.isWhitespace(codePoint)) {
                inPosition++;
                continue;
            }
            if (polygonCount > 0) {
                if (codePoint == ')') {
                    // right parenthesis,MULTI_POLYGON end.
                    inBuffer.position(inPosition + 1);
                    multiPolygonEnd = true;
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
            inBuffer.position(inPosition);
            //2-1 write POLYGON WKB header.
            writerIndex = writeGeometryHeader(outWrapper, elementType);
            //2-2 write POLYGON text WKB.
            linearRingCount = polygonTextToWkb(inBuffer, outWrapper, elementType);
            //2-3 write LinearRing count.
            if (linearRingCount != 0) {
                writeInt(outChannel, writerIndex, outWrapper.bigEndian, linearRingCount);
            }
            inPosition = inBuffer.position();
            polygonCount++;
        }

        if (!multiPolygonEnd) {
            throw new IllegalArgumentException(String.format("%s not close.", wkbType));
        }
        return polygonCount;
    }

    /**
     * @param inBuffer must be created by {@link ByteBuffer#wrap(byte[])} method.
     * @param wkbType  <ul>
     *                 <li>{@link WkbType#GEOMETRY_COLLECTION}</li>
     *                 <li>{@link WkbType#GEOMETRY_COLLECTION_Z}</li>
     *                 <li>{@link WkbType#GEOMETRY_COLLECTION_M}</li>
     *                 <li>{@link WkbType#GEOMETRY_COLLECTION_ZM}</li>
     *                 </ul>
     * @return geometry count.
     */
    protected static int geometryCollectionTextToWkb(final ByteBuffer inBuffer, final WkbMemoryWrapper outWrapper
            , final WkbType wkbType) {

        if (wkbType.family() != WkbType.GEOMETRY_COLLECTION) {
            throw createUnsupportedWkb(wkbType);
        }
        if (prepareGeometryText(inBuffer, outWrapper, wkbType)) {
            return 0;
        }
        final ByteBuffer outBuffer = outWrapper.buffer;
        final ByteBuf outChannel = outWrapper.outChannel;

        final int inLimit = inBuffer.limit();
        final byte[] inArray = inBuffer.array();
        int geometryCount = 0;
        WkbType geometryType;
        //2. write GEOMETRY wkb
        boolean geometryCollectionEnd = false;
        for (int inPosition = inBuffer.position(), codePoint, elementWriterIndex, elementCount; inPosition < inLimit; ) {
            codePoint = inArray[inPosition];
            if (Character.isWhitespace(codePoint)) {
                inPosition++;
                continue;
            }
            if (geometryCount > 0) {
                if (codePoint == ')') {
                    // right parenthesis,GEOMETRY_COLLECTION end.
                    inBuffer.position(inPosition + 1);
                    geometryCollectionEnd = true;
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
            inBuffer.position(inPosition);
            geometryType = readWkbFromWkt(inBuffer);
            if (geometryType == null || !geometryType.sameDimension(wkbType)) {
                throw createWktFormatError(wkbType.wktType);
            }
            elementWriterIndex = writeGeometryHeader(outWrapper, geometryType);
            outBuffer.flip();
            if (outBuffer.hasRemaining()) {
                outChannel.writeBytes(outBuffer);
            }
            outBuffer.clear();
            switch (geometryType.family()) {
                case POINT: {
                    pointTextToWkb(inBuffer, outWrapper, geometryType);

                    outBuffer.flip();
                    outChannel.writerIndex(elementWriterIndex);
                    outChannel.writeBytes(outBuffer);
                    outBuffer.clear();

                    inPosition = inBuffer.position();
                    geometryCount++;
                }
                continue;
                case LINE_STRING:
                    elementCount = lineStringTextToWkb(inBuffer, outWrapper, geometryType);
                    break;
                case POLYGON:
                    elementCount = polygonTextToWkb(inBuffer, outWrapper, geometryType);
                    break;
                case MULTI_POINT:
                    elementCount = multiPointTextToWkb(inBuffer, outWrapper, geometryType);
                    break;
                case MULTI_LINE_STRING:
                    elementCount = multiLineStringTextToWkb(inBuffer, outWrapper, geometryType);
                    break;
                case MULTI_POLYGON:
                    elementCount = multiPolygonTextToWkb(inBuffer, outWrapper, geometryType);
                    break;
                case GEOMETRY_COLLECTION:
                    elementCount = geometryCollectionTextToWkb(inBuffer, outWrapper, geometryType);
                    break;
                default:
                    throw createUnsupportedWkb(geometryType);
            }
            writeInt(outChannel, elementWriterIndex, outWrapper.bigEndian, elementCount);
            geometryCount++;
            inPosition = inBuffer.position();
        }
        if (!geometryCollectionEnd) {
            throw new IllegalArgumentException(String.format("%s not close.", wkbType));
        }
        return geometryCount;
    }

    /**
     * @return new offset
     * @see #lineStringToWkt(byte[], int[])
     */
    static int lineStringTextToWkt(final StringBuilder builder, final byte[] wkbArray, final WkbType wkbType
            , int offset) {
        if (wkbType.family() != WkbType.LINE_STRING) {
            throw createUnsupportedWkb(wkbType);
        }
        if (wkbArray.length < offset + HEADER_LENGTH) {
            throw createWkbLengthError(wkbType.wktType, wkbArray.length, offset + HEADER_LENGTH);
        }
        final boolean bigEndian = wkbArray[offset] == 0;
        offset += 5;
        final int pointCount = JdbdNumbers.readIntFromEndian(bigEndian, wkbArray, offset, 4);
        offset += 4;
        if (pointCount < 0) {
            throw createIllegalElementCount(pointCount);
        } else if (pointCount == 0) {
            builder.append(" EMPTY");
            return offset;
        }
        final int coordinates = wkbType.coordinates();
        if (wkbArray.length < offset + (coordinates << 3)) {
            throw createWkbLengthError(wkbType.wktType, wkbArray.length, offset + ((long) coordinates << 3));
        }
        builder.append("(");
        for (int i = 0; i < pointCount; i++) {
            if (i > 0) {
                builder.append(",");
            }
            for (int j = 0; j < coordinates; j++) {
                if (j > 0) {
                    builder.append(" ");
                }
                builder.append(JdbdNumbers.readDoubleFromEndian(bigEndian, wkbArray, offset, 8));
                offset += 8;
            }
        }
        builder.append(")");
        return offset;
    }

    /**
     * @return new offset
     * @see #multiPointToWkt(byte[], int[])
     * @see #geometryCollectionTextToWkt(StringBuilder, byte[], WkbType, int)
     */
    static int multiPointTextToWkt(final StringBuilder builder, final byte[] wkbArray, final WkbType wkbType
            , int offset) {
        if (wkbType.family() != WkbType.MULTI_POINT) {
            throw createUnsupportedWkb(wkbType);
        }
        boolean bigEndian = wkbArray[offset] == 0;
        offset += 5;
        final int pointCount = JdbdNumbers.readIntFromEndian(bigEndian, wkbArray, offset, 4);
        offset += 4;
        if (pointCount < 0) {
            throw createIllegalElementCount(pointCount);
        } else if (pointCount == 0) {
            builder.append(" EMPTY");
            return offset;
        }
        final int coordinates = wkbType.coordinates();
        final int needBytes = (5 + (coordinates << 3)) * pointCount;
        if (wkbArray.length < offset + needBytes) {
            throw createWkbLengthError(wkbType.wktType, wkbArray.length, offset + needBytes);
        }

        builder.append("(");
        // output MultiPoint
        final WkbType elementType = wkbType.elementType();
        WkbType pointType;
        for (int i = 0; i < pointCount; i++) {
            if (i > 0) {
                builder.append(",");
            }
            pointType = WkbType.fromWkbArray(wkbArray, offset);
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
                builder.append(JdbdNumbers.readDoubleFromEndian(bigEndian, wkbArray, offset, 8));
                offset += 8;
            }
            builder.append(")");
        }
        builder.append(")");

        return offset;
    }

    /**
     * @return new offset
     * @see #multiLineStringTextToWkt(StringBuilder, byte[], WkbType, int)
     * @see #geometryCollectionTextToWkt(StringBuilder, byte[], WkbType, int)
     */
    static int multiLineStringTextToWkt(final StringBuilder builder, final byte[] wkbArray, final WkbType wkbType
            , int offset) {
        if (wkbType.family() != WkbType.MULTI_LINE_STRING) {
            throw createUnsupportedWkb(wkbType);
        }
        boolean bigEndian = wkbArray[offset] == 0;
        offset += 5;
        final int lineStringCount = JdbdNumbers.readIntFromEndian(bigEndian, wkbArray, offset, 4);
        offset += 4;
        if (lineStringCount < 0) {
            throw createIllegalElementCount(lineStringCount);
        } else if (lineStringCount == 0) {
            builder.append(" EMPTY");
            return offset;
        }
        final int coordinates = wkbType.coordinates(), coordinateBytes = coordinates << 3;
        builder.append("(");
        // output MultiPoint
        final WkbType elementType = Objects.requireNonNull(wkbType.elementType());
        WkbType lineStringType;
        for (int i = 0, pointCount; i < lineStringCount; i++) {
            if (wkbArray.length < offset + HEADER_LENGTH) {
                throw createWkbLengthError(wkbType.wktType, wkbArray.length, offset + HEADER_LENGTH);
            }
            if (i > 0) {
                builder.append(",");
            }
            lineStringType = WkbType.fromWkbArray(wkbArray, offset);
            if (lineStringType != elementType) {
                throw new IllegalArgumentException(String.format("Error element type[%s],should be type[%s]."
                        , lineStringType, elementType));
            }
            bigEndian = wkbArray[offset] == 0;
            offset += 5;
            pointCount = JdbdNumbers.readIntFromEndian(bigEndian, wkbArray, offset, 4);
            offset += 4;
            if (pointCount < 0) {
                throw createIllegalLinearPointCountError(pointCount);
            } else if (pointCount == 0) {
                builder.append(" EMPTY");
                continue;
            }
            if (wkbArray.length < offset + coordinateBytes * pointCount) {
                throw createWkbLengthError(wkbType.wktType, wkbArray.length
                        , offset + (long) coordinateBytes * pointCount);
            }
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
                    builder.append(JdbdNumbers.readDoubleFromEndian(bigEndian, wkbArray, offset, 8));
                    offset += 8;
                }
            }
            builder.append(")");
        }
        builder.append(")");
        return offset;
    }

    /**
     * @return new offset
     * @see #multiPolygonToWkt(byte[], int[])
     * @see #geometryCollectionTextToWkt(StringBuilder, byte[], WkbType, int)
     */
    static int multiPolygonTextToWkt(final StringBuilder builder, final byte[] wkbArray, final WkbType wkbType
            , int offset) {
        if (wkbType.family() != WkbType.MULTI_POLYGON) {
            throw createUnsupportedWkb(wkbType);
        }
        final boolean bigEndian = wkbArray[offset] == 0;
        offset += 5;
        final int polygonCount = JdbdNumbers.readIntFromEndian(bigEndian, wkbArray, offset, 4);
        offset += 4;
        if (polygonCount < 0) {
            throw createIllegalElementCount(polygonCount);
        } else if (polygonCount == 0) {
            builder.append(" EMPTY");
            return offset;
        }
        builder.append("(");
        // output MULTI_POLYGON
        final WkbType elementType = Objects.requireNonNull(wkbType.elementType());
        WkbType polygonType;

        for (int i = 0; i < polygonCount; i++) {
            if (wkbArray.length < offset + HEADER_LENGTH) {
                throw createWkbLengthError(wkbType.wktType, wkbArray.length, offset + HEADER_LENGTH);
            }
            polygonType = WkbType.fromWkbArray(wkbArray, offset);
            if (polygonType != elementType) {
                throw new IllegalArgumentException(String.format("Not %s WKB", wkbType));
            }
            if (i > 0) {
                builder.append(",");
            }
            offset = polygonTextToWkt(builder, elementType, wkbArray, offset);
        }
        builder.append(")");
        return offset;
    }


    /**
     * @return new offset
     * @see #geometryCollectionTextToWkt(StringBuilder, byte[], WkbType, int)
     */
    static int geometryCollectionTextToWkt(final StringBuilder builder, final byte[] wkbArray, final WkbType wkbType
            , int offset) {
        if (wkbType.family() != WkbType.GEOMETRY_COLLECTION) {
            throw createUnsupportedWkb(wkbType);
        }
        if (wkbArray.length < offset + HEADER_LENGTH) {
            throw createWkbLengthError(wkbType.wktType, wkbArray.length, offset + HEADER_LENGTH);
        }
        boolean bigEndian = wkbArray[offset] == 0;
        offset += 5;
        final int geometryCount;
        geometryCount = JdbdNumbers.readIntFromEndian(bigEndian, wkbArray, offset, 4);
        offset += 4;
        if (geometryCount < 0) {
            throw createIllegalElementCount(geometryCount);
        } else if (geometryCount == 0) {
            builder.append(" EMPTY");
            return offset;
        }
        builder.append("(");
        WkbType geometryType;
        for (int i = 0; i < geometryCount; i++) {
            if (wkbArray.length < offset + HEADER_LENGTH) {
                throw createWkbLengthError(wkbType.wktType, wkbArray.length, offset + HEADER_LENGTH);
            }
            geometryType = WkbType.fromWkbArray(wkbArray, offset);
            if (!geometryType.sameDimension(wkbType)) {
                throw new IllegalArgumentException("WKB error.");
            }
            if (i > 0) {
                builder.append(",");
            }
            builder.append(geometryType.wktType);
            switch (geometryType.family()) {
                case POINT:
                    offset = pointTextToWkt(builder, wkbArray, geometryType, offset);
                    break;
                case LINE_STRING:
                    offset = lineStringTextToWkt(builder, wkbArray, geometryType, offset);
                    break;
                case POLYGON:
                    offset = polygonTextToWkt(builder, geometryType, wkbArray, offset);
                    break;
                case MULTI_POINT:
                    offset = multiPointTextToWkt(builder, wkbArray, geometryType, offset);
                    break;
                case MULTI_LINE_STRING:
                    offset = multiLineStringTextToWkt(builder, wkbArray, geometryType, offset);
                    break;
                case MULTI_POLYGON:
                    offset = multiPolygonTextToWkt(builder, wkbArray, geometryType, offset);
                    break;
                case GEOMETRY_COLLECTION:
                    offset = geometryCollectionTextToWkt(builder, wkbArray, geometryType, offset);
                    break;
                default:
                    throw createUnsupportedWkb(geometryType);
            }

        }
        builder.append(")");
        return offset;
    }

    /**
     * @param inBuffer must be created by {@link ByteBuffer#wrap(byte[])} method.
     */
    static void readNonNullWkb(final ByteBuffer inBuffer, final WkbType expectType) {
        final WkbType wkbType;
        wkbType = readWkbFromWkt(inBuffer);
        if (wkbType != expectType) {
            throw new IllegalArgumentException(String.format("WKT isn't %s .", expectType));
        }
    }

    /**
     * @param inBuffer must be created by {@link ByteBuffer#wrap(byte[])} method.
     * @see #readNonNullWkb(ByteBuffer, WkbType)
     */
    @Nullable
    static WkbType readWkbFromWkt(final ByteBuffer inBuffer) {
        final byte[] inArray = inBuffer.array();
        final int inLimit = inBuffer.limit();

        WkbType wkbType = null;
        for (int codePoint, inPosition = inBuffer.position(), endIndex = -1; inPosition < inLimit; ) {
            codePoint = inArray[inPosition];
            if (Character.isWhitespace(codePoint)) {
                inPosition++;
                continue;
            }
            for (int i = inPosition + 1; i < inLimit; i++) {
                codePoint = inArray[i];
                if (Character.isWhitespace(codePoint) || codePoint == '(') {
                    endIndex = i;
                    break;
                }
            }
            if (endIndex < 0) {
                break;
            }
            String wkt = new String(Arrays.copyOfRange(inArray, inPosition, endIndex));
            wkbType = WkbType.fromWkt(wkt);
            if (wkbType == null) {
                throw new IllegalArgumentException(String.format("Not found WKB type for %s .", wkt));
            }
            inBuffer.position(endIndex);
            break;
        }
        return wkbType;
    }


    static int writeGeometryHeader(final WkbMemoryWrapper outWrapper, final WkbType wkbType) {
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


    static byte checkByteOrder(byte byteOrder) {
        if (byteOrder != 0 && byteOrder != 1) {
            throw createIllegalByteOrderError(byteOrder);
        }
        return byteOrder;
    }


    static void writeInt(ByteBuf buffer, int writerIndex, boolean bigEndian, int intNum) {
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
     * @param inBuffer must be created by {@link ByteBuffer#wrap(byte[])} method.
     * @return count of points.
     */
    protected static int readAndWritePoints(ByteBuffer inBuffer, final WkbOUtWrapper outWrapper
            , final WkbType wkbType) throws IllegalArgumentException {

        final int coordinates = wkbType.coordinates();
        final boolean pointText = wkbType.supportPointText(), bigEndian = outWrapper.bigEndian;

        final byte[] inArray = inBuffer.array(), outArray = outWrapper.buffer.array();
        final ByteBuffer outBuffer = outWrapper.buffer;

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
            tempInPosition = inPosition;
            tempOutPosition = outPosition;
            if (pointCount > 0) {
                if (codePoint == ')') {
                    // right parenthesis. lineString end.
                    break;
                } else if (codePoint != ',') {
                    // linearRing separator
                    throw createWktFormatError(wkbType.wktType);
                }
                tempInPosition++;
                for (; tempInPosition < inLimit; tempInPosition++) {
                    codePoint = inArray[tempInPosition];
                    if (!Character.isWhitespace(codePoint)) {
                        break;
                    }
                }
            }
            if (pointText) {
                if (codePoint != '(') {
                    throw new IllegalArgumentException("Not found '(' for point text.");
                }
                tempInPosition++;
            }
            for (; tempInPosition < inLimit; tempInPosition++) {
                codePoint = inArray[tempInPosition];
                if (!Character.isWhitespace(codePoint)) {
                    break;
                }
            }
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
                JdbdNumbers.doubleToEndian(bigEndian, d, outArray, tempOutPosition);
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
     * @param inBuffer must be created by {@link ByteBuffer#wrap(byte[])} method.
     * @return LinearRing count
     */
    protected static int polygonTextToWkb(final ByteBuffer inBuffer, final WkbMemoryWrapper outWrapper
            , final WkbType wkbType) {
        if (wkbType.family() != WkbType.POLYGON) {
            throw createUnsupportedWkb(wkbType);
        }
        if (prepareGeometryText(inBuffer, outWrapper, wkbType)) {
            return 0;
        }

        final ByteBuffer outBuffer = outWrapper.buffer;
        final ByteBuf outChannel = outWrapper.outChannel;
        final int inLimit = inBuffer.limit();

        int linearRingCount = 0;
        boolean polygonEnd = false;
        for (int inPosition = inBuffer.position(); inPosition < inLimit; ) {
            linearRingCount += readAndWriteLinearRing(inBuffer, outWrapper, wkbType);

            outBuffer.flip();
            outChannel.writeBytes(outBuffer);
            outBuffer.clear();

            inPosition = inBuffer.position();
            if (inBuffer.get(inPosition) == ')') {
                polygonEnd = true;
                inBuffer.get(); // skip
                break;
            }

        }
        if (!polygonEnd) {
            throw new IllegalArgumentException(String.format("%s not close.", wkbType.wktType));
        }
        if (linearRingCount < 1) {
            throw new IllegalArgumentException(String.format("%s LinearRing count < 1", linearRingCount));
        }
        return linearRingCount;
    }


    /**
     * @param inBuffer must be created by {@link ByteBuffer#wrap(byte[])} method.
     * @return count of LinearRing
     * @see #readAndWritePoints(ByteBuffer, WkbOUtWrapper, WkbType)
     */
    protected static int readAndWriteLinearRing(final ByteBuffer inBuffer, final WkbMemoryWrapper outWrapper
            , final WkbType wkbType) {

        final byte[] inArray = inBuffer.array();
        final ByteBuffer outBuffer = outWrapper.buffer;
        final ByteBuf outChannel = outWrapper.outChannel;

        final byte[] startPoint = new byte[wkbType.coordinates() << 3], endPoint = new byte[startPoint.length];

        final int inLimit = inBuffer.limit();
        int codePoint, inPosition = inBuffer.position();
        int linearRingCount = 0;
        boolean linearRingEnd;
        for (int pointWriterIndex, pointCount, tempInPosition; inPosition < inLimit; ) {
            codePoint = inArray[inPosition];
            if (Character.isWhitespace(codePoint)) {
                inPosition++;
                continue;
            }

            if (linearRingCount > 0) {
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
            tempInPosition = inPosition;
            if (codePoint != '(') {
                // LinearRing start
                throw createWktFormatError(wkbType.wktType);
            }
            tempInPosition++;
            inBuffer.position(tempInPosition);

            pointWriterIndex = outChannel.writerIndex();
            outChannel.writeZero(4);// write placeholder of pointCount
            pointCount = 0;
            linearRingEnd = false;
            for (int j = 0, outPosition, oldPosition; tempInPosition < inLimit; j++) {
                outPosition = outBuffer.position();
                pointCount += readAndWritePoints(inBuffer, outWrapper, wkbType);
                if (outBuffer.position() == outPosition) {
                    throw createWktFormatError(wkbType.wktType);
                }
                if (j == 0 && pointCount > 0) {
                    oldPosition = outBuffer.position();
                    outBuffer.position(outPosition);
                    outBuffer.get(startPoint);
                    outBuffer.position(oldPosition);
                }
                tempInPosition = inBuffer.position();
                if (inBuffer.get(tempInPosition) == ')') {
                    tempInPosition++;
                    inBuffer.position(tempInPosition);
                    linearRingEnd = true;
                    linearRingCount++;
                }
                if (linearRingEnd && pointCount > 0) {
                    oldPosition = outBuffer.position();
                    outBuffer.position(outBuffer.position() - endPoint.length);
                    outBuffer.get(endPoint);
                    outBuffer.position(oldPosition);
                    if (!Arrays.equals(startPoint, endPoint)) {
                        throw createWktFormatError(wkbType.wktType);
                    }
                }
                outBuffer.flip();
                outChannel.writeBytes(outBuffer);
                outBuffer.clear();

                if (linearRingEnd) {
                    break;
                }

            }
            if (!linearRingEnd) {
                throw new IllegalArgumentException("LinearRing not close.");
            }
            if (pointCount < 4) {
                throw createWktFormatError(wkbType.wktType);
            }
            writeInt(outChannel, pointWriterIndex, outWrapper.bigEndian, pointCount); // write count of point.
            inPosition = tempInPosition;
        }
        inBuffer.position(inPosition);
        return linearRingCount;
    }

    /**
     * @param inBuffer must be created by {@link ByteBuffer#wrap(byte[])} method.
     */
    static void assertWhitespaceSuffix(ByteBuffer inBuffer, WkbType wkbType) {
        final byte[] inArray = inBuffer.array();
        for (int i = inBuffer.position(); i < inArray.length; i++) {
            if (!Character.isWhitespace(inArray[i])) {
                throw createWktFormatError(wkbType.wktType);
            }
        }
    }

    static boolean isEmptySet(ByteBuffer inBuffer) {
        final byte[] inArray = inBuffer.array();
        int offset = inBuffer.position();
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
            inBuffer.position(offset);
        }
        return match;
    }

    static byte[] channelToByteArray(ByteBuf outChannel) {
        byte[] wkb = new byte[outChannel.readableBytes()];
        outChannel.readBytes(wkb);
        return wkb;
    }

    static String createEmptySet(WkbType wkbType) {
        return wkbType.wktType + " EMPTY";
    }

    /**
     * @param inBuffer must be created by {@link ByteBuffer#wrap(byte[])} method.
     * @return true : empty set.
     */
    static boolean prepareGeometryText(final ByteBuffer inBuffer, final WkbMemoryWrapper outWrapper
            , WkbType wkbType) {

        skipWhitespace(inBuffer);
        if (isEmptySet(inBuffer)) {
            return true;
        }
        if (inBuffer.get() != '(') { // skip  left parenthesis.
            throw createWktFormatError(wkbType.wktType);
        }
        outWrapper.buffer.flip();
        if (outWrapper.buffer.hasRemaining()) {
            outWrapper.outChannel.writeBytes(outWrapper.buffer);
        }
        outWrapper.buffer.clear();
        return false;
    }


    /**
     * @param inBuffer must be created by {@link ByteBuffer#wrap(byte[])} method.
     */
    static void skipWhitespace(final ByteBuffer inBuffer) {
        final byte[] inArray = inBuffer.array();
        final int inLimit = inBuffer.limit();

        int inPosition = inBuffer.position();
        for (; inPosition < inLimit; inPosition++) {
            if (!Character.isWhitespace(inArray[inPosition])) {
                break;
            }
        }
        inBuffer.position(inPosition);
    }

    static IllegalArgumentException createIllegalByteOrderError(byte byteOrder) {
        return new IllegalArgumentException(String.format("Illegal byteOrder[%s].", byteOrder));
    }

    static IllegalArgumentException createIllegalWkbLengthError(long length, long expectLength) {
        return new IllegalArgumentException(String.format("WKB length[%s] but expect min length[%s]."
                , length, expectLength));
    }


    static IllegalArgumentException createWkbLengthError(String type, long length, long exceptLength) {
        return new IllegalArgumentException(String.format("WKB length[%s] and %s except length[%s] not match."
                , length, type, exceptLength));
    }

    static IllegalArgumentException createUnsupportedWkb(WkbType wkbType) {
        return new IllegalArgumentException(String.format("Unsupported WKB[%s]", wkbType));
    }


    static IllegalArgumentException createOffsetError(int offset, int arrayLength) {
        return new IllegalArgumentException(String.format("offset[%s] not in [0,%s).", offset, arrayLength));
    }

    static IllegalArgumentException createIllegalElementCount(int elementCount) {
        return new IllegalArgumentException(String.format("elementCount[%s] error", elementCount));
    }


    static IllegalArgumentException createIllegalLinearPointCountError(int pointCount) {
        return new IllegalArgumentException(String.format("LinearRing pointCount[%s] error", pointCount));
    }


    static IllegalArgumentException createNonLinearRingError(int linearRingIndex) {
        return new IllegalArgumentException(String.format("Polygon LinearRing[%s] isn't LinearRing", linearRingIndex));
    }

    static IllegalArgumentException createNonDoubleError(String nonDouble) {
        return new IllegalArgumentException(String.format("%s isn't double number.", nonDouble));
    }


    static IllegalArgumentException createWktFormatError(String wktType) {
        return new IllegalArgumentException(String.format("Not %s WKT format.", wktType));
    }

    static IllegalArgumentException createNonFamilyError(WkbType wkbType, int offset) {
        return new IllegalArgumentException(String.format("Non %s family type ,offset[%s]", wkbType, offset));
    }


    protected static class WkbOUtWrapper {

        public final ByteBuffer buffer;

        public final boolean bigEndian;

        public WkbOUtWrapper(int arrayLength, boolean bigEndian) {
            this.buffer = ByteBuffer.wrap(new byte[arrayLength]);
            this.bigEndian = bigEndian;
        }

    }

    protected static class WkbMemoryWrapper extends WkbOUtWrapper {

        public final ByteBuf outChannel;

        public WkbMemoryWrapper(int arrayLength, boolean bigEndian) {
            super(arrayLength, bigEndian);
            this.outChannel = ByteBufAllocator.DEFAULT.buffer(arrayLength, (1 << 30));
        }

    }




    /*################################## blow private method ##################################*/

    /**
     * @see #wkbEquals(byte[], byte[])
     */
    private static int geometryEquals(final byte[] geometryOne, final byte[] geometryTwo, int offset) {
        final WkbType wkbType;
        wkbType = WkbType.fromWkbArray(geometryOne, offset);
        if (wkbType != WkbType.fromWkbArray(geometryTwo, offset)) {
            return -1;
        }
        switch (wkbType.family()) {
            case POINT:
                offset = pointEquals(geometryOne, geometryTwo, offset);
                break;
            case LINE_STRING:
                offset = lineStringEquals(geometryOne, geometryTwo, offset);
                break;
            case POLYGON:
                offset = polygonEquals(geometryOne, geometryTwo, offset);
                break;
            case MULTI_POINT:
                offset = multiPointEquals(geometryOne, geometryTwo, offset);
                break;
            case MULTI_LINE_STRING:
                offset = multiLineStringEquals(geometryOne, geometryTwo, offset);
                break;
            case MULTI_POLYGON:
                offset = multiPolygonEquals(geometryOne, geometryTwo, offset);
                break;
            case GEOMETRY_COLLECTION:
                offset = geometryCollectionEquals(geometryOne, geometryTwo, offset);
                break;
            default:
                throw new IllegalArgumentException(String.format("not support %s now.", wkbType));
        }
        return offset;
    }

    /**
     * @see #geometryEquals(byte[], byte[], int)
     */
    private static int geometryCollectionEquals(final byte[] geometryOne, final byte[] geometryTwo, int offset) {
        final int geometryCount = checkHeader(geometryOne, geometryTwo, WkbType.GEOMETRY_COLLECTION, offset);
        offset += HEADER_LENGTH;
        if (geometryCount < 0) {
            return -1;
        } else if (geometryCount == 0) {
            return offset;
        }
        WkbType wkbType;
        for (int i = 0; i < geometryCount; i++) {
            wkbType = WkbType.fromWkbArray(geometryOne, offset);
            if (wkbType != WkbType.fromWkbArray(geometryTwo, offset)) {
                offset = -1;
                break;
            }
            offset = geometryEquals(geometryOne, geometryTwo, offset);
            if (offset < 0) {
                break;
            }
        }
        return offset;
    }


    private static int pointEquals(final byte[] pointOne, final byte[] pointTwo, int offset) {
        final WkbType wkbType;
        wkbType = WkbType.fromWkbArray(pointOne, offset);
        if (wkbType != WkbType.fromWkbArray(pointTwo, offset)) {
            return -1;
        }
        if (wkbType.family() != WkbType.POINT) {
            throw createNonFamilyError(WkbType.POINT, offset);
        }
        final boolean sameEndian = pointOne[offset] == pointTwo[offset];
        offset += 5;
        final int coordinates = wkbType.coordinates(), coordinateBytes = (coordinates << 3);
        if (pointOne.length < offset + coordinateBytes) {
            throw createWkbLengthError(wkbType.wktType, pointOne.length, offset + coordinateBytes);
        }
        if (sameEndian) {
            if (!JdbdArrays.equals(pointOne, pointTwo, offset, coordinateBytes)) {
                offset = -1;
            }
        } else if (!JdbdArrays.reverseEquals(pointOne, pointTwo, offset, 8, coordinates)) {
            offset = -1;
        }
        if (offset > 0) {
            offset += coordinateBytes;
        }
        return offset;
    }

    private static int lineStringEquals(final byte[] lineStringOne, final byte[] lineStringTwo, int offset) {
        final int elementCount = checkHeader(lineStringOne, lineStringTwo, WkbType.LINE_STRING, offset);
        if (elementCount < 0) {
            return -1;
        } else if (elementCount == 0) {
            return offset + HEADER_LENGTH;
        }
        final WkbType wkbType = WkbType.fromWkbArray(lineStringOne, offset);
        final boolean sameEndian = lineStringOne[offset] == lineStringTwo[offset];
        offset += HEADER_LENGTH;
        final int coordinates = wkbType.coordinates(), coordinateBytes = (coordinates << 3) * elementCount;
        if (sameEndian) {
            if (!JdbdArrays.equals(lineStringOne, lineStringTwo, offset, coordinateBytes)) {
                offset = -1;
            }
        } else if (!JdbdArrays.reverseEquals(lineStringOne, lineStringTwo, offset, 8, coordinates * elementCount)) {
            offset = -1;
        }
        if (offset > 0) {
            offset += coordinateBytes;
        }
        return offset;
    }


    /**
     * @see #multiPolygonEquals(byte[], byte[], int)
     */
    private static int polygonEquals(final byte[] polygonOne, final byte[] polygonTwo, int offset) {
        final int headerOffset = offset;
        final int elementCount = checkHeader(polygonOne, polygonTwo, WkbType.POLYGON, offset);
        offset += HEADER_LENGTH;
        if (elementCount < 0) {
            return -1;
        } else if (elementCount == 0) {
            return offset;
        }
        final WkbType wkbType = WkbType.fromWkbArray(polygonOne, headerOffset);
        final boolean sameEndian = polygonOne[headerOffset] == polygonTwo[headerOffset];


        final int coordinates = wkbType.coordinates(), coordinateBytes = coordinates << 3;
        for (int i = 0, pointCount, coordinateTotalBytes; i < elementCount; i++) {
            pointCount = checkElementCount(polygonOne, polygonTwo, headerOffset, offset);
            offset += 4;
            if (pointCount < 0) {
                offset = -1;
                break;
            } else if (pointCount == 0) {
                continue;
            }
            coordinateTotalBytes = pointCount * coordinateBytes;
            if (polygonOne.length < offset + coordinateTotalBytes) {
                throw createWkbLengthError(wkbType.wktType, polygonOne.length, offset + coordinateTotalBytes);
            }
            if (sameEndian) {
                if (!JdbdArrays.equals(polygonOne, polygonTwo, offset, coordinateTotalBytes)) {
                    offset = -1;
                    break;
                }
            } else if (!JdbdArrays.reverseEquals(polygonOne, polygonTwo, offset, 8, coordinates * pointCount)) {
                offset = -1;
                break;
            }
            offset += coordinateTotalBytes;
        }
        return offset;
    }


    /**
     * @see #geometryEquals(byte[], byte[], int)
     */
    private static int multiPointEquals(final byte[] multiPointOne, final byte[] multiPointTwo, int offset) {
        final int elementCount = checkHeader(multiPointOne, multiPointTwo, WkbType.MULTI_POINT, offset);
        offset += HEADER_LENGTH;
        if (elementCount < 0) {
            offset = -1;
        } else if (elementCount != 0) {
            for (int i = 0; i < elementCount; i++) {
                offset = pointEquals(multiPointOne, multiPointTwo, offset);
                if (offset < 0) {
                    break;
                }
            }
        }
        return offset;
    }

    /**
     * @see #geometryEquals(byte[], byte[], int)
     */
    private static int multiLineStringEquals(final byte[] multiLineStringOne, final byte[] multiLineStringTwo
            , int offset) {
        final int elementCount = checkHeader(multiLineStringOne, multiLineStringTwo, WkbType.MULTI_LINE_STRING, offset);
        offset += HEADER_LENGTH;
        if (elementCount < 0) {
            offset = -1;
        } else if (elementCount != 0) {
            for (int i = 0; i < elementCount; i++) {
                offset = lineStringEquals(multiLineStringOne, multiLineStringTwo, offset);
                if (offset < 0) {
                    break;
                }
            }
        }
        return offset;
    }


    /**
     * @see #geometryEquals(byte[], byte[], int)
     */
    private static int multiPolygonEquals(final byte[] multiPolygonOne, final byte[] multiPolygonTwo, int offset) {
        final int elementCount = checkHeader(multiPolygonOne, multiPolygonTwo, WkbType.MULTI_POLYGON, offset);
        offset += HEADER_LENGTH;
        if (elementCount < 0) {
            offset = -1;
        } else if (elementCount != 0) {
            for (int i = 0; i < elementCount; i++) {
                offset = polygonEquals(multiPolygonOne, multiPolygonTwo, offset);
                if (offset < 0) {
                    break;
                }
            }
        }
        return offset;

    }

    private static int checkElementCount(final byte[] geometryOne, final byte[] geometryTwo
            , final int headerOffset, final int offset) {
        final boolean match;
        if (geometryOne[headerOffset] == geometryTwo[headerOffset]) {
            match = JdbdArrays.equals(geometryOne, geometryTwo, offset, 4);
        } else {
            match = JdbdArrays.reverseEquals(geometryOne, geometryTwo, offset, 4, 1);
        }
        int elementCount;
        if (match) {
            if (geometryOne.length < offset + 4) {
                WkbType wkbType = WkbType.fromWkbArray(geometryOne, headerOffset);
                throw createWkbLengthError(wkbType.wktType, geometryOne.length, offset + 4);
            }
            elementCount = JdbdNumbers.readIntFromEndian(geometryOne[headerOffset] == 0, geometryOne, offset, 4);
            if (elementCount < 0) {
                throw createIllegalElementCount(elementCount);
            }
        } else {
            elementCount = -1;
        }
        return elementCount;
    }

    private static int checkHeader(final byte[] geometryOne, final byte[] geometryTwo, final WkbType expectFamily
            , int offset) {
        WkbType wkbTypeOne, wkbTypeTwo;
        wkbTypeOne = WkbType.fromWkbArray(geometryOne, offset);
        wkbTypeTwo = WkbType.fromWkbArray(geometryTwo, offset);
        if (wkbTypeOne != wkbTypeTwo) {
            return -1;
        }
        if (wkbTypeOne.family() != expectFamily) {
            throw createNonFamilyError(expectFamily, offset);
        }
        return checkElementCount(geometryOne, geometryTwo, offset, offset + 5);
    }


}
