package io.jdbd.vendor.util;

import org.qinarmy.util.BufferWrapper;
import org.qinarmy.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;


/**
 * This class design for geometry sql type test.
 *
 * @see <a href="https://www.ogc.org/standards/sfa">Simple Feature Access - Part 1: Common Architecture PDF</a>
 */
public abstract class Geometries extends GenericGeometries {


    private final static Logger LOG = LoggerFactory.getLogger(Geometries.class);

    public static final byte WKB_POINT_BYTES = 21;

    public static byte[] geometryToWkb(final String wktText, final boolean bigEndian) {
        BufferWrapper inWrapper = new BufferWrapper(wktText.getBytes(StandardCharsets.US_ASCII));

        final WkbType wkbType = readWkb(inWrapper);
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


    public static byte[] pointToWkb(final String pointWkt, final boolean bigEndian) {
        final BufferWrapper inWrapper = new BufferWrapper(pointWkt.getBytes(StandardCharsets.US_ASCII));

        final WkbOUtWrapper outWrapper;
        outWrapper = new WkbOUtWrapper(WKB_POINT_BYTES, bigEndian);

        final WkbType wkbType = WkbType.POINT;
        readNonNullWkb(inWrapper, wkbType);

        outWrapper.bufferArray[0] = outWrapper.bigEndian ? (byte) 0 : (byte) 1;
        JdbdNumberUtils.intToEndian(outWrapper.bigEndian, wkbType.code, outWrapper.bufferArray, 1, 4);
        outWrapper.buffer.position(5);

        int pointCount;
        pointCount = pointTextToWkb(inWrapper, outWrapper, wkbType);
        assertWhitespaceSuffix(inWrapper, wkbType);

        outWrapper.buffer.flip();
        final byte[] wkbArray;
        if (pointCount == 1) {
            wkbArray = outWrapper.bufferArray;
        } else {
            wkbArray = new byte[5];
            outWrapper.buffer.get(wkbArray);
        }
        return wkbArray;
    }

    public static Pair<Double, Double> readPointAsPair(final byte[] wkbArray, int offset) {
        if (offset < 0 || offset >= wkbArray.length) {
            throw createOffsetError(offset, wkbArray.length);
        }
        if (wkbArray.length - offset < WKB_POINT_BYTES) {
            throw createIllegalWkbLengthError(wkbArray.length, offset + WKB_POINT_BYTES);
        }

        final boolean bigEndian = checkByteOrder(wkbArray[offset++]) == 0;
        final int wkbType = JdbdNumberUtils.readIntFromEndian(bigEndian, wkbArray, offset, 4);
        if (wkbType != WkbType.POINT.code) {
            throw createWktFormatError(WkbType.POINT.name());
        }
        offset += 4;
        final double x, y;
        x = JdbdNumberUtils.readDoubleFromEndian(bigEndian, wkbArray, offset, 8);
        offset += 8;
        y = JdbdNumberUtils.readDoubleFromEndian(bigEndian, wkbArray, offset, 8);
        return new Pair<>(x, y);
    }


    public static byte[] lineStringToWkb(final String wktText, final boolean bigEndian) {
        BufferWrapper inWrapper = new BufferWrapper(wktText.getBytes(StandardCharsets.US_ASCII));
        WkbMemoryWrapper outWrapper = new WkbMemoryWrapper(256, bigEndian);

        final WkbType expectType = WkbType.LINE_STRING;
        readNonNullWkb(inWrapper, expectType);

        final int pointCount, headerIndex;
        headerIndex = writeGeometryHeader(outWrapper, expectType);
        pointCount = lineStringTextToWkb(inWrapper, outWrapper, expectType);
        assertWhitespaceSuffix(inWrapper, expectType);

        if (pointCount != 0) {
            writeInt(outWrapper.outChannel, headerIndex, outWrapper.bigEndian, pointCount);
        }
        return channelToByteArray(outWrapper.outChannel);
    }

    public static byte[] polygonToWkb(final String wktText, final boolean bigEndian) {
        BufferWrapper inWrapper = new BufferWrapper(wktText.getBytes(StandardCharsets.US_ASCII));
        WkbMemoryWrapper outWrapper = new WkbMemoryWrapper(256, bigEndian);

        final WkbType wkbType = WkbType.POLYGON;
        readNonNullWkb(inWrapper, wkbType);

        final int writerIndex, linearRingCount;
        writerIndex = writeGeometryHeader(outWrapper, wkbType);
        linearRingCount = polygonTextToWkb(inWrapper, outWrapper, wkbType);
        assertWhitespaceSuffix(inWrapper, wkbType);
        if (linearRingCount != 0) {
            writeInt(outWrapper.outChannel, writerIndex, bigEndian, linearRingCount);
        }
        return channelToByteArray(outWrapper.outChannel);
    }


    public static byte[] multiPointToWkb(final String wktText, final boolean bigEndian) {
        BufferWrapper inWrapper = new BufferWrapper(wktText.getBytes(StandardCharsets.US_ASCII));
        WkbMemoryWrapper outWrapper = new WkbMemoryWrapper(256, bigEndian);

        final WkbType expectType = WkbType.MULTI_POINT;
        readNonNullWkb(inWrapper, expectType);

        final int writerIndex, pointCount;
        writerIndex = writeGeometryHeader(outWrapper, expectType);
        pointCount = multiPointTextToWkb(inWrapper, outWrapper, expectType);
        assertWhitespaceSuffix(inWrapper, expectType);

        if (pointCount != 0) {
            writeInt(outWrapper.outChannel, writerIndex, bigEndian, pointCount);
        }
        return channelToByteArray(outWrapper.outChannel);
    }

    public static byte[] multiLineStringToWkb(final String wktText, final boolean bigEndian) {
        BufferWrapper inWrapper = new BufferWrapper(wktText.getBytes(StandardCharsets.US_ASCII));
        WkbMemoryWrapper outWrapper = new WkbMemoryWrapper(256, bigEndian);

        final WkbType expectType = WkbType.MULTI_LINE_STRING;
        readNonNullWkb(inWrapper, expectType);

        final int writerIndex, elementCount;

        writerIndex = writeGeometryHeader(outWrapper, expectType);
        elementCount = multiLineStringTextToWkb(inWrapper, outWrapper, expectType);
        assertWhitespaceSuffix(inWrapper, expectType);
        if (elementCount != 0) {
            writeInt(outWrapper.outChannel, writerIndex, bigEndian, elementCount);
        }
        return channelToByteArray(outWrapper.outChannel);
    }

    public static byte[] multiPolygonToWkb(final String wktText, final boolean bigEndian) {
        BufferWrapper inWrapper = new BufferWrapper(wktText.getBytes(StandardCharsets.US_ASCII));
        WkbMemoryWrapper outWrapper = new WkbMemoryWrapper(256, bigEndian);

        final WkbType wkbType = WkbType.MULTI_POLYGON;
        readNonNullWkb(inWrapper, wkbType);

        final int writerIndex, polygonCount;

        writerIndex = writeGeometryHeader(outWrapper, wkbType);
        polygonCount = multiPolygonTextToWkb(inWrapper, outWrapper, wkbType);
        assertWhitespaceSuffix(inWrapper, wkbType);
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
        BufferWrapper inWrapper = new BufferWrapper(wktText.getBytes(StandardCharsets.US_ASCII));
        WkbMemoryWrapper outWrapper = new WkbMemoryWrapper(256, bigEndian);

        final WkbType wktType = WkbType.GEOMETRY_COLLECTION;
        readNonNullWkb(inWrapper, wktType);

        final int elementCountWriterIndex, geometryCount;
        elementCountWriterIndex = writeGeometryHeader(outWrapper, wktType);
        geometryCount = geometryCollectionTextToWkb(inWrapper, outWrapper, wktType);

        assertWhitespaceSuffix(inWrapper, wktType);
        if (geometryCount != 0) {
            writeInt(outWrapper.outChannel, elementCountWriterIndex, outWrapper.bigEndian, geometryCount);
        }
        return channelToByteArray(outWrapper.outChannel);
    }

    /*################################## blow protected method ##################################*/



    /*################################## blow private method ##################################*/


}
