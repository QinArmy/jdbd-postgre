package io.jdbd.type;

import io.jdbd.lang.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

/**
 * @see <a href="https://www.ogc.org/standards/sfa">Simple Feature Access - Part 1: Common Architecture PDF</a>
 */
public enum WkbType implements CodeEnum {

    GEOMETRY(Constant.GEOMETRY),
    POINT(Constant.POINT),
    LINESTRING(Constant.LINESTRING),
    POLYGON(Constant.POLYGON),

    MULTIPOINT(Constant.MULTIPOINT),
    MULTILINESTRING(Constant.MULTILINESTRING),
    MULTIPOLYGON(Constant.MULTIPOLYGON),
    GEOMETRYCOLLECTION(Constant.GEOMETRYCOLLECTION),

    CIRCULARSTRING(Constant.CIRCULARSTRING),
    COMPOUNDCURVE(Constant.COMPOUNDCURVE),
    CURVEPOLYGON(Constant.CURVEPOLYGON),
    MULTICURVE(Constant.MULTICURVE),

    MULTISURFACE(Constant.MULTISURFACE),
    CURVE(Constant.CURVE),
    SURFACE(Constant.SURFACE),
    POLYHEDRALSURFACE(Constant.POLYHEDRALSURFACE),

    TIN(Constant.TIN);


    private static final Map<Integer, WkbType> CODE_MAP = CodeEnum.getCodeMap(WkbType.class);


    public final int code;

    WkbType(int code) {
        this.code = code;
    }

    @Override
    public int code() {
        return this.code;
    }


    public interface Constant {

        byte GEOMETRY = 0;
        byte POINT = 1;
        byte LINESTRING = 2;
        byte POLYGON = 3;

        byte MULTIPOINT = 4;
        byte MULTILINESTRING = 5;
        byte MULTIPOLYGON = 6;
        byte GEOMETRYCOLLECTION = 7;

        byte CIRCULARSTRING = 8;
        byte COMPOUNDCURVE = 9;
        byte CURVEPOLYGON = 10;
        byte MULTICURVE = 11;

        byte MULTISURFACE = 12;
        byte CURVE = 13;
        byte SURFACE = 14;
        byte POLYHEDRALSURFACE = 15;

        byte TIN = 16;

    }


    @Nullable
    public static WkbType resolve(int code) {
        return CODE_MAP.get(code);
    }

    public static WkbType resolveWkbType(final byte[] wkbArray) throws IllegalArgumentException {
        final byte byteOrder = wkbArray[0];
        if (byteOrder != 0 && byteOrder != 1) {
            throw new IllegalArgumentException(String.format("Illegal byteOrder[%s].", byteOrder));
        }
        int typeCode = readInt(byteOrder == 1, wkbArray);
        WkbType wkbType = resolve(typeCode);
        if (wkbType == null) {
            throw new IllegalArgumentException(String.format("Unknown WKB type[%s].", typeCode));
        }
        return wkbType;
    }


    public static WkbType resolveWkbType(final Path path) throws IllegalArgumentException, IOException {
        try (FileChannel in = FileChannel.open(path, StandardOpenOption.READ)) {
            byte[] bufferArray = new byte[5];
            ByteBuffer buffer = ByteBuffer.wrap(bufferArray);
            if (in.read(buffer) < 5) {
                throw new IllegalArgumentException("Empty path");
            }
            buffer.flip();
            return resolveWkbType(bufferArray);
        } catch (IOException | IllegalArgumentException e) {
            throw e;
        } catch (Throwable e) {
            throw new IOException(e.getMessage(), e);
        }
    }


    private static int readInt(final boolean bigEndian, byte[] wkbArray) {
        if (wkbArray.length < 5) {
            throw new IllegalArgumentException("WKB length < 5 .");
        }
        final int num;
        int offset = 1;
        if (bigEndian) {
            num = ((wkbArray[offset++] & 0xFF) << 24)
                    | ((wkbArray[offset++] & 0xFF) << 16)
                    | ((wkbArray[offset++] & 0xFF) << 8)
                    | (wkbArray[offset] & 0xFF);
        } else {
            num = (wkbArray[offset++] & 0xFF)
                    | ((wkbArray[offset++] & 0xFF) << 8)
                    | ((wkbArray[offset++] & 0xFF) << 16)
                    | ((wkbArray[offset] & 0xFF) << 24);
        }
        return num;
    }


}
