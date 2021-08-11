package io.jdbd.vendor.util;

import io.jdbd.type.geometry.Point;
import io.jdbd.type.geometry.WkbType;
import org.qinarmy.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;


/**
 * This class design for geometry sql type test.
 *
 * @see <a href="https://www.ogc.org/standards/sfa">Simple Feature Access - Part 1: Common Architecture PDF</a>
 */
public abstract class GeometryUtils extends GenericGeometries {


    private final static Logger LOG = LoggerFactory.getLogger(GeometryUtils.class);

    public static final byte WKB_POINT_BYTES = 21;

    public static String pointToWkt(Point point) {
        return new StringBuilder("POINT(")
                .append(point.getX())
                .append(" ")
                .append(point.getY())
                .toString();
    }

    public static boolean parseEndian(byte endianByte) {
        final boolean bigEndian;
        if (endianByte == 0) {
            bigEndian = true;
        } else if (endianByte == 1) {
            bigEndian = false;
        } else {
            throw new IllegalArgumentException("Error endian order byte.");
        }
        return bigEndian;
    }

    public static byte[] geometryToWkb(final String wktText, final boolean bigEndian) {
        ByteBuffer inBuffer = ByteBuffer.wrap(wktText.getBytes(StandardCharsets.US_ASCII));

        final WkbType wkbType = readWkbFromWkt(inBuffer);
        if (wkbType == null) {
            throw new IllegalArgumentException("wktText not geometry.");
        }
        final byte[] wkbArray;
        switch (wkbType) {
            case POINT:
                wkbArray = pointToWkb(wktText, bigEndian);
                break;
            case LINE_STRING:
                wkbArray = lineStringToWkb(wktText, bigEndian);
                break;
            case POLYGON:
                wkbArray = polygonToWkb(wktText, bigEndian);
                break;
            case MULTI_POINT:
                wkbArray = multiPointToWkb(wktText, bigEndian);
                break;
            case MULTI_LINE_STRING:
                wkbArray = multiLineStringToWkb(wktText, bigEndian);
                break;
            case MULTI_POLYGON:
                wkbArray = multiPolygonToWkb(wktText, bigEndian);
                break;
            case GEOMETRY_COLLECTION:
                wkbArray = geometryCollectionToWkb(wktText, bigEndian);
                break;
            default:
                throw createUnsupportedWkb(wkbType);
        }
        return wkbArray;
    }

    /**
     * @param pointWkt like POINT(0,0)
     */
    public static byte[] pointToWkb(final String pointWkt, final boolean bigEndian) {
        return doPointToWkb(pointWkt, bigEndian, true);
    }

    /**
     * @param pointValue like (0,0)
     */
    public static byte[] pointValueToWkb(String pointValue, final boolean bigEndian) {
        return doPointToWkb(pointValue, bigEndian, false);
    }


    public static Pair<Double, Double> readPointAsPair(final byte[] wkbArray, int offset) {
        if (offset < 0 || offset >= wkbArray.length) {
            throw createOffsetError(offset, wkbArray.length);
        }
        if (wkbArray.length - offset < WKB_POINT_BYTES) {
            throw createIllegalWkbLengthError(wkbArray.length, offset + WKB_POINT_BYTES);
        }

        final boolean bigEndian = checkByteOrder(wkbArray[offset++]) == 0;
        final int wkbType = JdbdNumbers.readIntFromEndian(bigEndian, wkbArray, offset, 4);
        if (wkbType != WkbType.POINT.code) {
            throw createWktFormatError(WkbType.POINT.name());
        }
        offset += 4;
        final double x, y;
        x = JdbdNumbers.readDoubleFromEndian(bigEndian, wkbArray, offset, 8);
        offset += 8;
        y = JdbdNumbers.readDoubleFromEndian(bigEndian, wkbArray, offset, 8);
        return new Pair<>(x, y);
    }


    public static byte[] lineStringToWkb(final String wktText, final boolean bigEndian) {
        ByteBuffer inBuffer = ByteBuffer.wrap(wktText.getBytes(StandardCharsets.US_ASCII));
        WkbMemoryWrapper outWrapper = new WkbMemoryWrapper(256, bigEndian);

        final WkbType expectType = WkbType.LINE_STRING;
        readNonNullWkb(inBuffer, expectType);

        final int pointCount, headerIndex;
        headerIndex = writeGeometryHeader(outWrapper, expectType);
        pointCount = lineStringTextToWkb(inBuffer, outWrapper, expectType);
        assertWhitespaceSuffix(inBuffer, expectType);

        if (pointCount != 0) {
            writeInt(outWrapper.outChannel, headerIndex, outWrapper.bigEndian, pointCount);
        }
        return channelToByteArray(outWrapper.outChannel);
    }

    public static byte[] polygonToWkb(final String wktText, final boolean bigEndian) {
        ByteBuffer inBuffer = ByteBuffer.wrap(wktText.getBytes(StandardCharsets.US_ASCII));
        WkbMemoryWrapper outWrapper = new WkbMemoryWrapper(256, bigEndian);

        final WkbType wkbType = WkbType.POLYGON;
        readNonNullWkb(inBuffer, wkbType);

        final int writerIndex, linearRingCount;
        writerIndex = writeGeometryHeader(outWrapper, wkbType);
        linearRingCount = polygonTextToWkb(inBuffer, outWrapper, wkbType);
        assertWhitespaceSuffix(inBuffer, wkbType);
        if (linearRingCount != 0) {
            writeInt(outWrapper.outChannel, writerIndex, bigEndian, linearRingCount);
        }
        return channelToByteArray(outWrapper.outChannel);
    }


    public static byte[] multiPointToWkb(final String wktText, final boolean bigEndian) {
        ByteBuffer inBuffer = ByteBuffer.wrap(wktText.getBytes(StandardCharsets.US_ASCII));
        WkbMemoryWrapper outWrapper = new WkbMemoryWrapper(256, bigEndian);

        final WkbType expectType = WkbType.MULTI_POINT;
        readNonNullWkb(inBuffer, expectType);

        final int writerIndex, pointCount;
        writerIndex = writeGeometryHeader(outWrapper, expectType);
        pointCount = multiPointTextToWkb(inBuffer, outWrapper, expectType);
        assertWhitespaceSuffix(inBuffer, expectType);

        if (pointCount != 0) {
            writeInt(outWrapper.outChannel, writerIndex, bigEndian, pointCount);
        }
        return channelToByteArray(outWrapper.outChannel);
    }

    public static byte[] multiLineStringToWkb(final String wktText, final boolean bigEndian) {
        ByteBuffer inBuffer = ByteBuffer.wrap(wktText.getBytes(StandardCharsets.US_ASCII));
        WkbMemoryWrapper outWrapper = new WkbMemoryWrapper(256, bigEndian);

        final WkbType expectType = WkbType.MULTI_LINE_STRING;
        readNonNullWkb(inBuffer, expectType);

        final int writerIndex, elementCount;

        writerIndex = writeGeometryHeader(outWrapper, expectType);
        elementCount = multiLineStringTextToWkb(inBuffer, outWrapper, expectType);
        assertWhitespaceSuffix(inBuffer, expectType);
        if (elementCount != 0) {
            writeInt(outWrapper.outChannel, writerIndex, bigEndian, elementCount);
        }
        return channelToByteArray(outWrapper.outChannel);
    }

    public static byte[] multiPolygonToWkb(final String wktText, final boolean bigEndian) {
        ByteBuffer inBuffer = ByteBuffer.wrap(wktText.getBytes(StandardCharsets.US_ASCII));
        WkbMemoryWrapper outWrapper = new WkbMemoryWrapper(256, bigEndian);

        final WkbType wkbType = WkbType.MULTI_POLYGON;
        readNonNullWkb(inBuffer, wkbType);

        final int writerIndex, polygonCount;

        writerIndex = writeGeometryHeader(outWrapper, wkbType);
        polygonCount = multiPolygonTextToWkb(inBuffer, outWrapper, wkbType);
        assertWhitespaceSuffix(inBuffer, wkbType);
        if (polygonCount != 0) {
            writeInt(outWrapper.outChannel, writerIndex, bigEndian, polygonCount);
        }
        return channelToByteArray(outWrapper.outChannel);
    }

    /**
     * @param wktText <ul>
     *                <li>{@link WkbType#GEOMETRY_COLLECTION}</li>
     *                <li>{@link WkbType#GEOMETRY_COLLECTION_Z}</li>
     *                <li>{@link WkbType#GEOMETRY_COLLECTION_M}</li>
     *                <li>{@link WkbType#GEOMETRY_COLLECTION_ZM}</li>
     *                </ul>
     */
    public static byte[] geometryCollectionToWkb(final String wktText, final boolean bigEndian) {
        ByteBuffer inBuffer = ByteBuffer.wrap(wktText.getBytes(StandardCharsets.US_ASCII));
        WkbMemoryWrapper outWrapper = new WkbMemoryWrapper(256, bigEndian);

        final WkbType wktType = WkbType.GEOMETRY_COLLECTION;
        readNonNullWkb(inBuffer, wktType);

        final int elementCountWriterIndex, geometryCount;
        elementCountWriterIndex = writeGeometryHeader(outWrapper, wktType);
        geometryCount = geometryCollectionTextToWkb(inBuffer, outWrapper, wktType);

        assertWhitespaceSuffix(inBuffer, wktType);
        if (geometryCount != 0) {
            writeInt(outWrapper.outChannel, elementCountWriterIndex, outWrapper.bigEndian, geometryCount);
        }
        return channelToByteArray(outWrapper.outChannel);
    }

    /*################################## blow protected method ##################################*/



    /*################################## blow private method ##################################*/

    /**
     * @see #pointToWkb(String, boolean)
     * @see #pointValueToWkb(String, boolean)
     */
    private static byte[] doPointToWkb(final String pointWkt, final boolean bigEndian, final boolean hasTag) {
        final ByteBuffer inBuffer = ByteBuffer.wrap(pointWkt.getBytes(StandardCharsets.US_ASCII));

        final WkbOUtWrapper outWrapper;
        outWrapper = new WkbOUtWrapper(WKB_POINT_BYTES, bigEndian);

        final WkbType wkbType = WkbType.POINT;
        if (hasTag) {
            readNonNullWkb(inBuffer, wkbType);
        }

        outWrapper.buffer.put(0, outWrapper.bigEndian ? (byte) 0 : (byte) 1);
        JdbdNumbers.intToEndian(outWrapper.bigEndian, wkbType.code, outWrapper.buffer.array(), 1, 4);
        outWrapper.buffer.position(5);

        int pointCount;
        pointCount = pointTextToWkb(inBuffer, outWrapper, wkbType);
        assertWhitespaceSuffix(inBuffer, wkbType);

        outWrapper.buffer.flip();

        final byte[] wkbArray;
        if (pointCount == 1) {
            wkbArray = outWrapper.buffer.array();
        } else {
            wkbArray = new byte[5];
            outWrapper.buffer.get(wkbArray);
        }
        return wkbArray;
    }


}
