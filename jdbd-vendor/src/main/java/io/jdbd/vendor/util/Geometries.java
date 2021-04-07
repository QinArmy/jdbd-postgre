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


    public static byte[] pointToWkb(final String pointWkt, final boolean bigEndian) {
        final BufferWrapper inWrapper = new BufferWrapper(pointWkt.getBytes(StandardCharsets.US_ASCII));

        final WkbOUtWrapper outWrapper;
        outWrapper = new WkbOUtWrapper(WKB_POINT_BYTES, bigEndian);

        final WkbType wkbType = WkbType.POINT;

        doPointToWkb(inWrapper, outWrapper, wkbType);
        assertWhitespaceSuffix(inWrapper, wkbType);

        outWrapper.buffer.flip();
        final byte[] wkbArray;
        if (outWrapper.buffer.remaining() == 5) {
            wkbArray = new byte[5];
            outWrapper.buffer.get(wkbArray);
        } else {
            wkbArray = outWrapper.bufferArray;
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
        WkbMemoryWrapper outWrapper = new WkbMemoryWrapper(inWrapper.bufferArray.length, bigEndian);

        final WkbType wkbType = WkbType.LINE_STRING;

        doLineStringToWkb(inWrapper, outWrapper, wkbType);
        assertWhitespaceSuffix(inWrapper, wkbType);

        byte[] wkb = new byte[outWrapper.outChannel.readableBytes()];
        outWrapper.outChannel.readBytes(wkb);
        return wkb;
    }

    public static byte[] polygonToWkb(final String wktText, final boolean bigEndian) {
        BufferWrapper inWrapper = new BufferWrapper(wktText.getBytes(StandardCharsets.US_ASCII));
        WkbMemoryWrapper outWrapper = new WkbMemoryWrapper(128, bigEndian);

        final WkbType wkbType = WkbType.POLYGON;

        doPolygonToWkb(inWrapper, outWrapper, wkbType);
        assertWhitespaceSuffix(inWrapper, wkbType);

        byte[] wkb = new byte[outWrapper.outChannel.readableBytes()];
        outWrapper.outChannel.readBytes(wkb);
        return wkb;
    }


    public static byte[] multiPointToWkb(final String wktText, final boolean bigEndian) {
        BufferWrapper inWrapper = new BufferWrapper(wktText.getBytes(StandardCharsets.US_ASCII));
        WkbMemoryWrapper outWrapper = new WkbMemoryWrapper(inWrapper.bufferArray.length, bigEndian);

        final WkbType wkbType = WkbType.MULTI_POINT;
        doMultiPointToWkb(inWrapper, outWrapper, wkbType);
        assertWhitespaceSuffix(inWrapper, wkbType);

        byte[] wkb = new byte[outWrapper.outChannel.readableBytes()];
        outWrapper.outChannel.readBytes(wkb);
        return wkb;
    }

    public static byte[] multiLineStringToWkb(final String wktText, final boolean bigEndian) {
        BufferWrapper inWrapper = new BufferWrapper(wktText.getBytes(StandardCharsets.US_ASCII));
        WkbMemoryWrapper outWrapper = new WkbMemoryWrapper(inWrapper.bufferArray.length, bigEndian);

        final WkbType wkbType = WkbType.MULTI_LINE_STRING;

        doMultiLineStringToWkb(inWrapper, outWrapper, wkbType);
        assertWhitespaceSuffix(inWrapper, wkbType);

        byte[] wkb = new byte[outWrapper.outChannel.readableBytes()];
        outWrapper.outChannel.readBytes(wkb);
        return wkb;
    }

    public static byte[] multiPolygonToWkb(final String wktText, final boolean bigEndian) {
        BufferWrapper inWrapper = new BufferWrapper(wktText.getBytes(StandardCharsets.US_ASCII));
        WkbMemoryWrapper outWrapper = new WkbMemoryWrapper(inWrapper.bufferArray.length, bigEndian);

        final WkbType wkbType = WkbType.MULTI_POLYGON;

        doMultiPolygonToWkb(inWrapper, outWrapper, wkbType);
        assertWhitespaceSuffix(inWrapper, wkbType);

        byte[] wkb = new byte[outWrapper.outChannel.readableBytes()];
        outWrapper.outChannel.readBytes(wkb);
        return wkb;
    }

    /*################################## blow protected method ##################################*/



    /*################################## blow private method ##################################*/


}
